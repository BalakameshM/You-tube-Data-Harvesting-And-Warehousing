#  Youtube Data Harvesting And Warehousing

Overview:

This project focuses on harvesting and warehousing YouTube data using Python, Google's YouTube API, MongoDB, MySQL, and Streamlit. The application fetches data about a specified YouTube channel, including channel details, video information, and comments. The collected data is then stored in both MongoDB and MySQL databases for further analysis.

Features:

YouTube API Integration with Python: Utilizes Google's YouTube API with Python to fetch detailed information about a specified YouTube channel, its videos, and comments.

MongoDB Integration: Stores the fetched data in a MongoDB database, providing a NoSQL solution for flexible and scalable data storage.

MySQL Integration: Migrates the harvested data from MongoDB to MySQL, leveraging the structured, relational database capabilities of MySQL. This allows for efficient querying and analysis of the stored data using SQL.

Streamlit Dashboard for Data Visualization: Utilizes Streamlit, a Python library for creating interactive web applications, to build a user-friendly dashboard. This dashboard allows users to visualize and explore the collected YouTube data in a seamless and intuitive way.

Data Analysis Queries with SQL: Provides pre-defined SQL queries for data analysis, enabling users to gain insights into the YouTube channel and video data.

Dependencies:

1) Google API Python Client.
2) pymongo.
3) pandas.
4) streamlit.
5) pymysql.
