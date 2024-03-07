%%writefile youtube_data_harvesting.py
import pymysql
import pymongo
import pandas as pd
from googleapiclient.discovery import build
import streamlit as st
from pprint import pprint
from datetime import timedelta

# connecting_to_Mongo_DB
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["youtube_data_harvesting"]

#function_to_get_channel_details
def get_channel_details(api_key, channel_id):
    youtube = build("youtube", "v3", developerKey=api_key)
    channel_details_list = []

    channel_response = youtube.channels().list(
        part="snippet,contentDetails,statistics,status",
        id=channel_id
    ).execute()

    for i in range(len(channel_response["items"])):
        channel_type = channel_response["items"][i]["snippet"].get("channelType", "N/A")
        channel_details = {
            "channel_Name": channel_response["items"][i]["snippet"]["title"],
            "channel_Id": channel_response["items"][i]["id"],
            "subscription_Count": channel_response["items"][i]["statistics"]["subscriberCount"],
            "channel_Views": channel_response["items"][i]["statistics"]["viewCount"],
            "channel_Description": channel_response["items"][i]["snippet"]["description"],
            "playlist_id": channel_response["items"][i]['contentDetails']['relatedPlaylists']['uploads'],
            "channel_Type": channel_type,
            "channel_Status": channel_response["items"][i]["status"]["privacyStatus"],
            "total_videos":channel_response["items"][0]["statistics"].get("videoCount", 0)
        }
        channel_details_list.append(channel_details)

    return channel_details_list

#function_to_get_video_ids
def get_video_ids(api_key, playlistId, max_results=50):
    youtube = build("youtube", "v3", developerKey=api_key)
    video_ids_list = []
    next_page_token = None

    while True:
        playlist_details = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlistId,
            maxResults=max_results,
            pageToken=next_page_token
        ).execute()

        for playlist_item in playlist_details["items"]:
            video_ids = playlist_item["snippet"]["resourceId"]["videoId"]
            video_ids_list.append(video_ids)

        next_page_token = playlist_details.get('nextPageToken')
        if not next_page_token:
            break

    return video_ids_list

#function_to_get_video_details
def get_video_information(api_key, channel_details):
    youtube = build("youtube", "v3", developerKey=api_key)
    video_information_list = []

    for channel in channel_details:
        playlist_id = channel["playlist_id"]
        channel_Id = channel["channel_Id"]
        video_ids = get_video_ids(api_key, playlist_id)

        for video_id in video_ids:
            video_details = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            ).execute()

            for item in video_details["items"]:
                video_information = {
                    "video_id": item['id'],
                    "channel_Id":channel_Id,
                    "playlist_id": playlist_id,
                    "video_name": item['snippet']['title'],
                    "video_description": item['snippet']['description'],
                    "published_date": item['snippet']['publishedAt'],
                    "view_count": item['statistics'].get('viewCount', 0),
                    "like_count": item['statistics'].get('likeCount', 0),
                    "favorite_count": item['statistics'].get('favoriteCount', 0),
                    "comment_count": int(item['statistics'].get('commentCount', 0)),
                    "duration": item['contentDetails']['duration'],
                    "thumbnails": item["snippet"]["thumbnails"],
                    "caption_status": item["contentDetails"]["caption"]
                }
                video_information_list.append(video_information)

    return video_information_list

#function_to_get_comments_details
def get_comment_information(api_key, video_ids_list, max_result=50):
    youtube = build("youtube", "v3", developerKey=api_key)
    comment_details_list = []

    for video_id in video_ids_list:
        try:
            comments = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=max_result
            ).execute()

            for comment_item in comments["items"]:
                comment_details = {
                    "comment_id": comment_item["snippet"]["topLevelComment"]["id"],
                    "comment_text": comment_item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    "Comment_author": comment_item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    "Comment_published_date": comment_item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                }
                comment_details_list.append(comment_details)
        except Exception as e:
            print(f"Comments are disabled for video {video_id}: {e}")

    return comment_details_list

#function_to_insert_data_in_Mongo_DB
def insert_data_in_mongo_db(api_key, channel_id):
    channel_details_list = get_channel_details(api_key, channel_id)
    if channel_details_list:
        video_information_list = get_video_information(api_key, channel_details_list)
        video_ids_list = [video_info["video_id"] for video_info in video_information_list]
        comment_information_list = get_comment_information(api_key, video_ids_list)

        collection_name = "channel_details"
        collection = db[collection_name]
        existing_document = db.channel_details.find_one({"channel_data.channel_Id": channel_details_list[0]["channel_Id"]})
        if existing_document:
            return None
    
        try: 
            result = collection.insert_one({
                "channel_data": channel_details_list[0],
                "video_information": video_information_list,
                "comment_data": comment_information_list
            })

            last_inserted_id = result.inserted_id
            
            print(f"Upload successfully for channel: {channel_id}")
        except Exception as e:
            print(f"Error inserting data into MongoDB: {e}")
    else:
        print(f"Error fetching channel data for channel: {channel_id}")
    
    return last_inserted_id

# functions_to_get_data_from_Mongo_DB App
def retrieve_data_from_mongo_db(inserted_id):
    collection_name = "channel_details"
    collection = db[collection_name]

    data_list = []

    cursor = collection.find({"_id": inserted_id})
    for document in cursor:
        channel_data = document.get("channel_data", {})
        video_information = document.get("video_information", [])
        comment_data = document.get("comment_data", [])

        data = {
            "channel_data": channel_data,
            "video_information": video_information,
            "comment_data": comment_data,
        }
        data_list.append(data)

    return data_list

#function_to_insert_data_from_Mongo_DB_to-MYSQL
def insert_data_into_phpmyadmin(data_list):
    connection = pymysql.connect(
        host="localhost",
        user="root",
        password="",
        database="youtube_harvesting"
    )
    
    cursor = connection.cursor()

    for data in data_list:
    
        channel_data = data.get("channel_data", {})
        cursor.execute(
            "INSERT INTO channel (channel_id, channel_name, channel_type, channel_views,channel_description,channel_status,total_videos,subscription_count) VALUES (%s, %s, %s, %s, %s,%s,%s,%s)",
            (channel_data.get("channel_Id"), channel_data.get("channel_Name"),
             channel_data.get("channel_Type"), channel_data.get("channel_Views"),
             channel_data.get("channel_Description"),channel_data.get("channel_Status"),
             channel_data.get("total_videos"),channel_data.get("subscription_Count"))
        )
        
        video_information_list = data.get("video_information", {})
        for video_info in video_information_list:
            cursor.execute("""
                INSERT INTO `video`(`video_id`,`channel_id`, `playlist_id`, `video_name`, `video_description`,
                                   `published_date`, `view_count`, `like_count`, `dislike_count`,
                                   `favorite_count`, `comment_count`, `duration`, `thumbnail`,
                                   `caption_status`)
                VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                video_info.get("video_id", ""),
                video_info.get("channel_Id", ""),
                video_info.get("playlist_id", ""),
                video_info.get("video_name", ""),
                video_info.get("video_description", ""),
                video_info.get("published_date", ""),
                video_info.get("view_count", 0),
                video_info.get("like_count", 0),
                video_info.get("dislike_count", 0),
                video_info.get("favorite_count", 0),
                video_info.get("comment_count", 0),
                video_info.get("duration"),
                video_info.get("thumbnails", {}).get("default", {}).get("url", ""),
                video_info.get("caption_status", "")
            ))
            comment_information_list = data.get("comment_data", {})
            for comment_info in comment_information_list:
                cursor.execute("""
                    INSERT INTO comment (comment_id, video_id, comment_text, comment_author, comment_published_date)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    comment_info.get("comment_id", ""),
                    video_info.get("video_id", ""),
                    comment_info.get("comment_text", ""),
                    comment_info.get("Comment_author", ""),
                    comment_info.get("Comment_published_date", "")
                ))

        connection.commit()
    cursor.close()
    connection.close()
           
def show_data_harvesting():
    st.title("YouTube Data Harvesting and Warehousing using SQL, MongoDB, and Streamlit")
    st.write("Overview:")
    st.write("This project focuses on harvesting and warehousing YouTube data using Python, Google's YouTube API, MongoDB, MySQL, and Streamlit. The application fetches data about a specified YouTube channel, including channel details, video information, and comments. The collected data is then stored in both MongoDB and MySQL databases for further analysis.")

    api_key = "AIzaSyCtz3sg-iSTRWoVWfAA-antD3ADb5i51DY"
    channel_id = st.text_input("Enter YouTube channel Id:")
    global inserted_id
    if api_key and channel_id:
        if st.button("Fetch and Insert Data to MongoDB"):
            with st.spinner("Fetching and inserting data to MongoDB..."):
                inserted_id = insert_data_in_mongo_db(api_key, channel_id)
                if inserted_id is not None:
                    data_list = retrieve_data_from_mongo_db(inserted_id)
                    st.success("Data Inserted To MongoDB successfully!")
                    st.title("Inserted Data to MongoDB")
                    st.write("Showing data from MongoDB collection:")
                    st.write(data_list)
                else:
                    st.error("Channel data with the given id is already there!")
                    
def main():

    if 'is_mysql_data_page' not in st.session_state:
        st.session_state.is_mysql_data_page = False
    if 'is_data_harvesting_page' not in st.session_state:
        st.session_state.is_data_harvesting_page = True
    if 'is_warehouse_data_page' not in st.session_state:
        st.session_state.is_warehouse_data_page = False
    if 'is_migrate_page' not in st.session_state:
        st.session_state.is_migrate_page = False
    st.sidebar.title("YouTube Data Harvesting with Streamlit")
    
    if st.sidebar.button("Add Channel details to Data lake"):
        st.session_state.is_mysql_data_page = False
        st.session_state.is_data_harvesting_page = True
        st.session_state.is_warehouse_data_page = False
        st.session_state.is_migrate_page = False
    if st.sidebar.button("Migrate to SQL"):
        st.session_state.is_mysql_data_page = False
        st.session_state.is_data_harvesting_page = False
        st.session_state.is_warehouse_data_page = False
        st.session_state.is_migrate_page = True
    if st.sidebar.button("SQL Data Warehouse"):
        st.session_state.is_warehouse_data_page = True
        st.session_state.is_mysql_data_page = False
        st.session_state.is_data_harvesting_page = False
        st.session_state.is_migrate_page = False
    if st.sidebar.button("Data Analysis With SQL Queries"):
        st.session_state.is_mysql_data_page = True
        st.session_state.is_data_harvesting_page = False
        st.session_state.is_warehouse_data_page = False
        st.session_state.is_migrate_page = False
        
    if st.session_state.is_mysql_data_page:
        show_mysql_data()
    elif st.session_state.is_data_harvesting_page:
        show_data_harvesting()
    elif st.session_state.is_warehouse_data_page:
        show_warehouse()
    elif st.session_state.is_migrate_page:
        show_migrate_page()
        
def show_migrate_page():
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["youtube_data_harvesting"]
    collection_name = "channel_details"
    collection = db[collection_name]

    channel_names = [doc["channel_data"]["channel_Name"] for doc in collection.find({}, {"channel_data.channel_Name": 1})]


    st.title("Select a Channel from MongoDB")
    selected_channel = st.selectbox("Select Channel:", channel_names)
    if selected_channel:
        if st.button('Migrate'):
            with st.spinner('Migrating Data to SQL'):
                st.write(f"Selected Channel: {selected_channel}")
                document = collection.find_one({"channel_data.channel_Name": selected_channel})
                channel_data = document.get("channel_data", {})

                connection = pymysql.connect(
                    host="localhost",
                    user="root",
                    password="",
                    database="youtube_harvesting"
                )
                cursor = connection.cursor()
                cursor.execute("SELECT * FROM channel WHERE channel_name = %s", (selected_channel,))
                existing_data = cursor.fetchone()

                if existing_data:
                    st.error("Data for the selected channel already exists in MySQL.")
                else:
                    data_list = []

                    channel_data = document.get("channel_data", {})
                    video_information = document.get("video_information", [])
                    comment_data = document.get("comment_data", [])

                    data = {
                        "channel_data": channel_data,
                        "video_information": video_information,
                        "comment_data": comment_data,
                    }
                    data_list.append(data)
                    insert_data_into_phpmyadmin(data_list)
                    st.success("Data inserted into MySQL successfully!")
    else:
        st.warning("Please select a channel.")
        
def show_warehouse():
    mydb = pymysql.connect(host="localhost",
            user="root",
            password="",
            database= "youtube_harvesting"
            )
    cursor = mydb.cursor()
    st.title("SQL Data Warehouse - select the table to view the details ")
    selected_table = st.radio("Select a table to view the details", ("channels","videos","comments"))
    if selected_table == "channels":
        query_channels = "SELECT channel_id,channel_name,total_videos,channel_views,channel_description,channel_status,subscription_count FROM channel;"
        cursor.execute(query_channels)
        mydb.commit()
        t_channels = cursor.fetchall()
        table_width = 800
        st.table(pd.DataFrame(t_channels, columns=["Channel ID", "Channel Name","Total Videos", "Channel Views", "Channel Description", "Channel Status","Subscription Count"]).style.set_table_styles([dict(selector="table", props=[("width", f"{table_width}px")])]))
        pass
    elif selected_table == "videos":
        query_channels = "SELECT video.video_id,video.video_name,video.video_description,channel.channel_name,video.view_count,video.playlist_id,video.like_count FROM `video` JOIN channel ON channel.channel_id = video.channel_id"
        cursor.execute(query_channels)
        mydb.commit()
        t_channels = cursor.fetchall()
        table_width = 800
        st.table(pd.DataFrame(t_channels, columns=["Video ID", "Video Name","Video Description", "Channel Name", "View Count", "Playlist Id", "Like Count"]).style.set_table_styles([dict(selector="table", props=[("width", f"{table_width}px")])]))
        pass
    elif selected_table == "comments":
        query_channels = "SELECT channel.channel_name,video.video_name,comment.comment_text,comment.comment_author FROM `comment` INNER JOIN video ON video.video_id = comment.video_id INNER JOIN channel ON channel.channel_id = video.channel_id LIMIT 200"
        cursor.execute(query_channels)
        mydb.commit()
        t_channels = cursor.fetchall()
        table_width = 800
        st.table(pd.DataFrame(t_channels, columns=["Channel Name", "Video Name","Comment", "Comment Author"]).style.set_table_styles([dict(selector="table", props=[("width", f"{table_width}px")])]))
        pass
    
def show_mysql_data():
    st.title("Data Analysis with SQL Queries")
    mydb = pymysql.connect(host="localhost",
            user="root",
            password="",
            database= "youtube_harvesting"
            )
    cursor = mydb.cursor()
    question = st.selectbox(
        'Please Select Your Question',
        ('1. All the videos and the Channel Name',
         '2. Channels with most number of videos',
         '3. 10 most viewed videos',
         '4. Comments in each video',
         '5. Videos with highest likes',
         '6. likes of all videos',
         '7. views of each channel',
         '8. videos published in the year 2022',
         '9. average duration of all videos in each channel',
         '10. videos with highest number of comments'))

    if question == '1. All the videos and the Channel Name':
        query1 = "SELECT video.video_name,channel.channel_name FROM `video` INNER JOIN channel ON channel.channel_id = video.channel_id;"
        cursor.execute(query1)
        mydb.commit()
        t1=cursor.fetchall()
        table_width = 800
        st.table(pd.DataFrame(t1, columns=["Video Title", "Channel Name"]).style.set_table_styles([dict(selector="table", props=[("width", f"{table_width}px")])]))

    elif question == '2. Channels with most number of videos':
        query2 = "SELECT channel.channel_name, COUNT(video.video_name) AS video_count FROM channel INNER JOIN video ON channel.channel_id = video.channel_id GROUP BY channel.channel_name ORDER BY COUNT(video.video_name) DESC;"
        cursor.execute(query2)
        mydb.commit()
        t2=cursor.fetchall()
        table_width = 800
        st.table(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]).style.set_table_styles([dict(selector="table", props=[("width", f"{table_width}px")])]))

    elif question == '3. 10 most viewed videos':
        query3 = "SELECT channel.channel_name, video.video_name, video.view_count FROM video INNER JOIN channel ON channel.channel_id = video.channel_id WHERE video.view_count is not null ORDER BY video.view_count DESC LIMIT 10;"
        cursor.execute(query3)
        mydb.commit()
        t3 = cursor.fetchall() 
        table_width = 800
        st.write(pd.DataFrame(t3, columns = ["Channel Name","Video Title","View Count"]).style.set_table_styles([dict(selector="table", props=[("width", f"{table_width}px")])]))

    elif question == '4. Comments in each video':
        query4 = "SELECT video.comment_count, video.video_name,channel.channel_name from video INNER JOIN channel ON channel.channel_id = video.channel_id;"
        cursor.execute(query4)
        mydb.commit()
        t4=cursor.fetchall()
        table_width = 800
        st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title","Channel Name"]).style.set_table_styles([dict(selector="table", props=[("width", f"{table_width}px")])]))

    elif question == '5. Videos with highest likes':
        query5 = "SELECT video.video_name AS VideoTitle, channel.channel_name AS ChannelName, video.like_count AS LikeCount FROM video INNER JOIN channel ON video.channel_id = channel.channel_id ORDER BY video.like_count DESC LIMIT 10;"
        cursor.execute(query5)
        mydb.commit()
        t5 = cursor.fetchall()
        table_width = 800
        st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]).style.set_table_styles([dict(selector="table", props=[("width", f"{table_width}px")])]))

    elif question == '6. likes of all videos':
        query6 = "select video.like_count as likeCount,video.video_name as VideoTitle,channel.channel_name from video INNER JOIN channel ON channel.channel_id = video.channel_id;;"
        cursor.execute(query6)
        mydb.commit()
        t6 = cursor.fetchall()
        table_width = 800
        st.write(pd.DataFrame(t6, columns=["like count","video title","Channel Name"]).style.set_table_styles([dict(selector="table", props=[("width", f"{table_width}px")])]))

    elif question == '7. views of each channel':
        query7 = "select channel_name as ChannelName, channel_views as Channelviews from channel;"
        cursor.execute(query7)
        mydb.commit()
        t7=cursor.fetchall()
        table_width = 800
        st.write(pd.DataFrame(t7, columns=["channel name","total views"]).style.set_table_styles([dict(selector="table", props=[("width", f"{table_width}px")])]))

    elif question == '8. videos published in the year 2022':
        query8 = '''select video.video_name as Video_Title, video.published_date as VideoRelease, channel.channel_name as ChannelName from video inner join channel 
                    ON channel.channel_id = video.channel_id where extract(year from video.published_date) = 2022;'''
        cursor.execute(query8)
        mydb.commit()
        t8=cursor.fetchall()
        table_width = 800
        st.write(pd.DataFrame(t8,columns=["Video Name", "Video Publised On", "ChannelName"]).style.set_table_styles([dict(selector="table", props=[("width", f"{table_width}px")])]))

    elif question == '9. average duration of all videos in each channel':
        query9 =  '''SELECT
        channel.channel_name AS ChannelName, SEC_TO_TIME(AVG(
            SUBSTRING_INDEX(SUBSTRING_INDEX(video.duration, 'PT', -1), 'S', 1) +
            SUBSTRING_INDEX(SUBSTRING_INDEX(video.duration, 'T', -1), 'M', 1) * 60 +
            IF(LOCATE('H', video.duration) > 0, SUBSTRING_INDEX(SUBSTRING_INDEX(video.duration, 'T', 1), 'H', -1) * 3600, 0)
        )) AS average_duration FROM channel INNER JOIN video ON video.channel_id = channel.channel_id GROUP BY channel.channel_name;'''
        cursor.execute(query9)
        mydb.commit()
        t9=cursor.fetchall()
        table_width = 800
        t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
        T9=[]
        for index, row in t9.iterrows():
            channel_title = row['ChannelTitle']
            average_duration = row['Average Duration']
            average_duration_str = str(average_duration)
            T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
        st.write(pd.DataFrame(T9))

    elif question == '10. videos with highest number of comments':
        query10 = '''SELECT v.video_name,c.channel_name, v.comment_count FROM video v JOIN channel c ON v.channel_id = c.channel_id ORDER BY v.comment_count DESC LIMIT 10;'''
        cursor.execute(query10)
        mydb.commit()
        t10=cursor.fetchall()
        table_width = 800
        st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']).style.set_table_styles([dict(selector="table", props=[("width", f"{table_width}px")])]))
if __name__ == "__main__":
    main()

#create_new_cell_and_run_the_below:
!streamlit run youtube_data_harvesting.py
