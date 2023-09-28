from pymongo import MongoClient
from mysql.connector import connect
import pandas as pd

# Please Enter the MYSQL Password
password = '*******'

client = MongoClient('mongodb://localhost:27017/')
mydb = client['youtube_scrapping_project']


sql_db = connect(
    host = 'localhost',
    user = 'root',
    password = password,
    auth_plugin = 'mysql_native_password'
)
mycursor = sql_db.cursor()

def mysql_video_list():
    video_list = []
    mycursor.execute('SELECT video_id from video')
    for each in mycursor:
        video_list.append(each[0])
    return video_list

def create_table():
    database_list = []
    mycursor.execute("show databases")

    for each in mycursor:
        database_list.append(each[0])

    if 'youtube_scrapping_project' not in database_list:
        mycursor.execute('create database youtube_scrapping_project')
        mycursor.execute('use youtube_scrapping_project')
        mycursor.execute("""CREATE TABLE 
        Channel(channel_id VARCHAR(255) PRIMARY KEY,channel_name VARCHAR(255),channel_type VARCHAR(255),channel_views BIGINT,channel_description TEXT,channel_status VARCHAR(255))""")
        mycursor.execute("""CREATE TABLE 
        Playlist(playlist_id VARCHAR(255) PRIMARY KEY,channel_id VARCHAR(255),playlist_name VARCHAR(255),FOREIGN KEY (channel_id) REFERENCES channel(channel_id))""")
        mycursor.execute("""CREATE TABLE
        Video(video_id VARCHAR(255) PRIMARY KEY,playlist_id VARCHAR(255),video_name VARCHAR(255),video_description TEXT,published_date DATETIME,view_count BIGINT,like_count INT,dislike_count INT,favourite_count INT,comment_count INT,duration INT,thumbnail VARCHAR(255),caption_status VARCHAR(255), FOREIGN KEY (playlist_id) REFERENCES playlist(playlist_id))""")
        mycursor.execute("""CREATE TABLE 
        Comment(comment_id VARCHAR(255) PRIMARY KEY,video_id VARCHAR(255),comment_text TEXT,comment_author VARCHAR(255),comment_published_date DATETIME, FOREIGN KEY (video_id) REFERENCES video (video_id))""")
        sql_db.commit()
    else:
        mycursor.execute('use youtube_scrapping_project')

def truncate():
    mycursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    mycursor.execute("TRUNCATE channel")
    mycursor.execute("TRUNCATE playlist")
    mycursor.execute("TRUNCATE comment")
    mycursor.execute("TRUNCATE video")
    mycursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    sql_db.commit()
    return "All the tables are truncated"

def migrate_into_channel(channel):
    channel_data = mydb[channel].find_one()["Channel_Name"]  
    mycursor.execute("INSERT INTO CHANNEL (channel_id,channel_name,channel_views,channel_description) VALUES (%s,%s,%s,%s)",(channel_data["Channel_Id"],channel_data["Channel_Name"],channel_data["Channel_Views"],channel_data["Channel_Description"]))

def check_sql_channels():
    sql_channels = [] 
    mycursor.execute("select channel_name from channel")
    for each in mycursor:
        sql_channels.append(each[0])
    return sql_channels

def migration(channel):
    migrate_into_channel(channel)
    for each_document in mydb[channel].find({},{"_id":0}):
        playlist_id = each_document["Channel_Name"]["Playlist_Id"]
        for each in each_document:
            if each_document[each].get("Channel_Name",0):
                channel_info = each_document[each]
                mycursor.execute("INSERT INTO playlist(playlist_id,channel_id,playlist_name) VALUES(%s,%s,%s)",(channel_info["Playlist_Id"],channel_info["Channel_Id"],channel_info["Playlist_Name"]))
            elif each_document[each].get("Video_Id",0) and each_document[each].get("Video_Id",0) not in mysql_video_list():
                video_data = each_document[each]
                mycursor.execute("INSERT INTO video(video_id,playlist_id,video_name,video_description,view_count,like_count,dislike_count,comment_count,duration,thumbnail,caption_status,published_date) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(video_data["Video_Id"],playlist_id,video_data["Video_Name"],video_data["Video_Description"],video_data["View_Count"],video_data["Like_Count"],video_data["Dislike_Count"],video_data["Comment_Count"],video_data["Duration"],video_data["Thumbnail"],video_data["Caption_Status"],video_data["PublishedAt"].replace('T',' ').replace('Z','')))
                video_id = each_document[each]["Video_Id"]
                comments = each_document[each]["Comments"]
                for each_comments in comments:
                    comment_info = comments[each_comments]
                    mycursor.execute("INSERT INTO comment(comment_id,video_id,comment_text,comment_author,comment_published_date) VALUES(%s,%s,%s,%s,%s)",(comment_info["Comment_Id"],video_id,comment_info["Comment_Text"],comment_info["Comment_Author"],comment_info["Comment_PublishedAt"].replace('T',' ').replace('Z','')))
    sql_db.commit()

def channel_list_query():
    channel_list = []
    create_table()
    mycursor.execute("select channel.channel_name from channel")
    for each in mycursor:
        channel_list.append(each)
    return channel_list

def migrate_to_sql(channel): 
    if channel not in check_sql_channels():
        migration(channel)
        return "Migrated successfully"
    else:
        return f"{channel} is already migrated"


def first_query():
    mycursor.execute("select video.video_name,channel.channel_name from channel inner join playlist on channel.channel_id = playlist.channel_id inner join video on playlist.playlist_id = video.playlist_id")
    first_query = []
    for each in mycursor:
        first_query.append(each)
    return (pd.DataFrame(first_query,columns=['Video Name','Channel Name']))

def second_query():
    mycursor.execute("""
    SELECT channel.channel_name, COUNT(video.video_id) as video_count 
    FROM channel 
    INNER JOIN playlist on channel.channel_id = playlist.channel_id 
    INNER JOIN video on playlist.playlist_id = video.playlist_id
    GROUP BY channel.channel_name
    ORDER BY video_count desc""")
    second_query = []
    for each in mycursor:
        second_query.append(each)
    return (pd.DataFrame(second_query,columns=["Channel Name","Number of Videos"]))

def third_query():
    mycursor.execute("""
    SELECT video.video_name,video.view_count,channel.channel_name  
    FROM video 
    INNER JOIN playlist ON video.playlist_id=playlist.playlist_id
    INNER JOIN channel ON channel.channel_id=playlist.channel_id 
    ORDER BY video.view_count DESC LIMIT 10""")
    third_query = []
    for each in mycursor:
        third_query.append(each)
    return (pd.DataFrame(third_query,columns=["Video Name","View Count","Channel Name"]))

def fourth_query():
    mycursor.execute("SELECT video.video_name,video.comment_count from video ORDER BY video.comment_count DESC")
    fourth_query = []
    for each in mycursor:
        fourth_query.append(each)
    return (pd.DataFrame(fourth_query,columns=["Video Name","Comments Count"]))

def fifth_query():
    mycursor.execute("""
    SELECT video.video_name,video.like_count,channel.channel_name FROM video 
    INNER JOIN playlist ON video.playlist_id=playlist.playlist_id 
    INNER JOIN channel ON playlist.channel_id=channel.channel_id 
    ORDER BY video.like_count DESC""")
    fifth_query=[]
    for each in mycursor:
        fifth_query.append(each)
    return (pd.DataFrame(fifth_query,columns=["Video Name","Like Count","Channel Name"]))

def sixth_query():
    mycursor.execute("SELECT video.video_name,(video.like_count + video.dislike_count) FROM video")
    sixth_query = []
    for each in mycursor:
        sixth_query.append(each)
    return (pd.DataFrame(sixth_query,columns=["Video Name","Total number of Likes and Dislikes"]))

def seventh_query():
    mycursor.execute("SELECT channel_name,channel_views FROM channel")
    seventh_query = []
    for each in mycursor:
        seventh_query.append(each)
    return (pd.DataFrame(seventh_query,columns=["Channel Name","Total Views"]))

def eighth_query():
    mycursor.execute("""
                SELECT DISTINCT channel.channel_name FROM channel 
                INNER JOIN playlist ON channel.channel_id = playlist.channel_id 
                INNER JOIN video ON video.playlist_id = playlist.playlist_id WHERE video.published_date LIKE '2022%';""")
    eighth_query = []
    for each in mycursor:
        eighth_query.append(each)
    return (pd.DataFrame(eighth_query,columns=["Channel Name"]))

def nighth_query():
    mycursor.execute("""
    SELECT channel.channel_name,AVG(video.duration) FROM video
    INNER JOIN playlist ON video.playlist_id = playlist.playlist_id 
    INNER JOIN channel ON playlist.channel_id=channel.channel_id 
    GROUP BY channel.channel_name""")
    nighth_query = []
    for each in mycursor:
        nighth_query.append(each)
    return (pd.DataFrame(nighth_query,columns=["Channel Name","Average Video Duration in Seconds"]))

def tenth_query():
    tenth_query=[]
    mycursor.execute("""
    SELECT channel.channel_name,video.video_name,video.comment_count FROM video 
    INNER JOIN playlist ON video.playlist_id = playlist.playlist_id
    INNER JOIN channel ON playlist.channel_id = channel.channel_id 
    ORDER BY video.comment_count DESC LIMIT 10""")
    for each in mycursor:
        tenth_query.append(each)
    return (pd.DataFrame(tenth_query,columns=["Channel Name","Video Name","Number of Comments"]))


    


    

    




     
    



    
    
    
