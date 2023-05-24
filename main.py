import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
import mysql.connector
from datetime import datetime

api_key='AIzaSyA-fonnlXtepFO6nNUNW5Un5LIH9e-zbSc'
youtube = build('youtube','v3',developerKey=api_key)

st.title('Youtube_Data_Harvesting')
keyword=st.sidebar.text_input("Enter your Channel_id: ")
def get_channel_stats(youtube, keyword):
    all_data = []
    request = youtube.channels().list(part='snippet,contentDetails, statistics', id=keyword)

    response = request.execute()
    for i in range(len(response['items'])):
        data = dict(channel_id=response['items'][0]['id'],
                    Channel_name=response['items'][0]['snippet']['title'],
                    Subscribers=response['items'][0]['statistics']['subscriberCount'],
                    Views=response['items'][0]['statistics']['viewCount'],
                    Total_videos=response['items'][0]['statistics']['videoCount'],
                    playlist_ids=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                    published=response['items'][0]['snippet']['publishedAt'],
                    description=response['items'][0]['snippet']['description'])

        all_data.append(data)

    return all_data
get_channel_stats(youtube, keyword)


channel_statistics=get_channel_stats(youtube,keyword)
channel_data=pd.DataFrame(channel_statistics)

for j in range(len(channel_data)):
    keyword1 = channel_data.loc[j,'playlist_ids']

def get_video_ids(youtube, keyword1):
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=keyword1,
        maxResults=50)

    response = request.execute()

    video_ids = []

    for i in range(0, len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    more_pages = True
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=keyword1,
                maxResults=50, pageToken=next_page_token)
            response = request.execute()

            for i in range(0, len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])

            next_page_token = response.get('nextPageToken')

    return video_ids


keyword2=get_video_ids(youtube,keyword1)


def get_video_details(youtube, keyword2):
    all_statistics = []

    for i in range(0, len(keyword2), 50):
        request = youtube.videos().list(
            part='contentDetails,snippet,statistics',
            id=keyword2[i:i + 50],
            maxResults=50)

        response = request.execute()

        for video in response['items']:
            video_stats = dict(channel_tile=video['snippet']['channelTitle'],
                               video_id=video['id'],
                               Title=video['snippet']['title'],
                               Published_date=video['snippet']['publishedAt'],
                               Duration=video['contentDetails']['duration'],
                               Views=video['statistics']['viewCount'],
                               Likes=video['statistics']['likeCount'],
                               Comment_count=video['statistics']['commentCount'],
                               Favourite_comment=video['statistics']['favoriteCount'])

            all_statistics.append(video_stats)

    return all_statistics
get_video_details(youtube,keyword2)


def get_all_comments(youtube, keyword2):
    all_comments = []

    for video_id in keyword2:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id, maxResults=5)

        try:
            response = request.execute()

            if response['items']:
                for comment in response['items']:
                    comment_id = comment['id']
                    comment_text = comment['snippet']['topLevelComment']['snippet']['textDisplay']
                    #                     comment_author=comment['snippet']['toplevelComment']['snippet']['authorDisplayName'],
                    comments_in_video_info = {'video_id': video_id, 'Comment_id': comment_id,
                                              'Comment_text': comment_text}

                    all_comments.append(comments_in_video_info)




        except:

            pass

    return all_comments

get_all_comments(youtube,keyword2)

if keyword:
    data=[get_channel_stats(youtube, keyword),get_video_details(youtube,keyword2),get_all_comments(youtube,keyword2)]
    st.write(data)

channel_data=pd.DataFrame(channel_statistics)
video_details=get_video_details(youtube,keyword2)
for x in range(len(channel_data)):
    channel_name=channel_data.loc[x,"Channel_name"]
vi=pd.DataFrame(video_details)
comments=get_all_comments(youtube,keyword2)
video_comments=pd.DataFrame(comments)
youtube_data = []
for i in range(len(channel_data)):
    data_1 = {"Channel": {"Channel_id": channel_data.loc[i, "channel_id"],
                        "Channel_name": channel_data.loc[i, "Channel_name"],
                        "Subscribers": channel_data.loc[i, "Subscribers"],
                        "Views": channel_data.loc[i, "Views"],
                        "Total_videos": channel_data.loc[i, "Total_videos"],
                        "Published": channel_data.loc[i, "published"],
                        "Description": channel_data.loc[i, "description"],
                        "play_list": channel_data.loc[i, "playlist_ids"],
                        "videos": []}}
    for j in range(len(vi)):
        video = {"video_id": vi.loc[j, "video_id"],
                 "Title": vi.loc[j, "Title"],
                 "Published_date": vi.loc[j, 'Published_date'],
                 "Duration": vi.loc[j, 'Duration'],
                 "views": vi.loc[j, "Views"],
                 "likes": vi.loc[j, "Likes"],
                 "Comment_count": vi.loc[j, 'Comment_count'],
                 "comments": []}
        data_1["Channel"]["videos"].append(video)

        for k in range(len(video_comments)):
            if vi.loc[j, "video_id"] == video_comments.loc[k, 'video_id']:
                comments = {"comment_id": video_comments.loc[k, 'Comment_id'],
                            "comment_text": video_comments.loc[k, "Comment_text"]}
                video["comments"].append(comments)

youtube_data.append(data_1)

myclient = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = myclient['youtube']
mycol = mydb[channel_name]

if st.button('Store in Mongodb'):
    mycol.insert_many(youtube_data)
list=[]
for coll in mydb.list_collection_names():
    list.append(coll)

if keyword:
    st.sidebar.multiselect('you searched',list)

vi['Published_date']=pd.to_datetime(vi['Published_date']).dt.date
vi['Duration']=pd.to_timedelta(vi['Duration']).dt.total_seconds()
vi['Duration']=vi['Duration'].apply(np.int64)

if st.button("migrate to my sql"):
    db=mysql.connector.connect(
        host='localhost',
        user='root',
        password='12345678',
        database='youtube_data_harvest'
    )

mycursor=db.cursor()
ch_table = mycursor.execute(
    "CREATE TABLE IF NOT EXISTS channels (channel_id VARCHAR(255) PRIMARY KEY, channel_name VARCHAR(255), subscribers int,views int,total_vidoes int)")
for i in range(len(channel_data)):
    details = "INSERT INTO channels (channel_id,channel_name,subscribers,views,total_vidoes) VALUES (%s, %s,%s,%s,%s)"
    val = (channel_data.loc[i, 'channel_id'],
           channel_data.loc[i, 'Channel_name'],
           channel_data.loc[i, 'Subscribers'],
           channel_data.loc[i, 'Views'],
           channel_data.loc[i, 'Total_videos'])

    ch_values = mycursor.execute(details, val)

db.commit()

pl_table = mycursor.execute(
    "CREATE TABLE IF NOT EXISTS playlist (playlist_id VARCHAR(255) PRIMARY KEY,channel_id VARCHAR(255),FOREIGN KEY (channel_id) REFERENCES channels(channel_id))")
for i in range(len(channel_data)):
    details = "INSERT INTO playlist (playlist_id,channel_id ) VALUES (%s, %s)"
    val = (channel_data.loc[i, 'playlist_ids'],
           channel_data.loc[i, 'channel_id'])

    pl_values = mycursor.execute(details, val)

db.commit()

vi_table = mycursor.execute(
    "CREATE TABLE IF NOT EXISTS video (video_id VARCHAR(255) PRIMARY KEY,playlist_id VARCHAR(255),FOREIGN KEY (playlist_id) REFERENCES playlist(playlist_id),Title text,Published_date datetime,Duration int,Views int, likes int,comment_count int)")
for j in range(len(channel_data)):
    for i in range(len(vi)):
        details = "INSERT INTO video (video_id,playlist_id,Title,Published_date,Duration,views,likes,comment_count) VALUES (%s, %s,%s,%s,%s,%s,%s,%s)"
        val = (vi.loc[i, 'video_id'],
               channel_data.loc[j, "playlist_ids"],
               vi.loc[i, 'Title'],
               vi.loc[i, 'Published_date'],
               int(vi.loc[i, 'Duration']),
               vi.loc[i, 'Views'],
               vi.loc[i, 'Likes'],
               vi.loc[i, 'Comment_count'])

        vi_values = mycursor.execute(details, val)

db.commit()

co_table=mycursor.execute("CREATE TABLE IF NOT EXISTS comment (comment_id VARCHAR(255) PRIMARY KEY,video_id VARCHAR(255), FOREIGN KEY (video_id) REFERENCES video(video_id),comment_text text)")
for i in range(len(video_comments)):
    details= "INSERT INTO comment(comment_id,video_id,comment_text) VALUES(%s,%s,%s)"
    val=(video_comments.loc[i,'Comment_id'],
         video_comments.loc[i,'video_id'],
         video_comments.loc[i,'Comment_text'])
    co_values=mycursor.execute(details,val)

db.commit()


    # sql_collections=[ch_table,ch_values,pl_table,pl_values,vi_table,vi_values,co_table,co_values]
    # st.write(sql_collections)

st.text_input('Questionnaire','Required solutions')

mycursor.execute("SELECT channels.channel_name, video.Title FROM channels JOIN playlist ON playlist.channel_id=channels.channel_id JOIN video ON video.playlist_id=playlist.playlist_id")
query_1=mycursor.fetchall()
df_1=pd.DataFrame(query_1,columns=['channels','video_name'])
st.table(df_1)

mycursor.execute("SELECT channel_name, total_vidoes FROM channels WHERE (total_vidoes =(SELECT MAX(total_vidoes) AS 'Maximum total_vidoes' FROM channels))")
query_2=mycursor.fetchall()
df_2=pd.DataFrame(query_2,columns=['channel_name','Most_Videos'])
st.table(df_2)

mycursor.execute(
    "SELECT channels.channel_name,video.Title,video.Views FROM channels JOIN playlist ON playlist.channel_id= channels.channel_id JOIN video ON video.playlist_id = playlist.playlist_id ORDER BY video.Views DESC LIMIT 10")
query_3 = mycursor.fetchall()
df_3 = pd.DataFrame(query_3, columns=['channel_name', 'video_name', 'Most_views'])
st.table(df_3)

mycursor.execute("SELECT  Title, comment_count FROM video ORDER BY comment_count")
query_4=mycursor.fetchall()
df_4=pd.DataFrame(query_4,columns=['Video_names','Comment_count'])
st.table(df_4)


mycursor.execute("SELECT channels.channel_name, video.Title, video.likes FROM channels JOIN playlist ON playlist.channel_id=channels.channel_id JOIN video ON video.playlist_id=playlist.playlist_id ORDER BY video.likes DESC LIMIT 20")
query_5=mycursor.fetchall()
df_5=pd.DataFrame(query_5, columns=['Channel_names','Video_names','Highest_likes'])
st.table(df_5)

mycursor.execute("SELECT video.Title, video.likes FROM video")
query_6=mycursor.fetchall()
df_6=pd.DataFrame(query_6, columns=['Video_names','likes'])
st.table(df_6)

mycursor.execute("SELECT channel_name,views FROM channels ORDER BY views DESC")
query_7=mycursor.fetchall()
df_7=pd.DataFrame(query_7,columns=['channel_name','views'])
st.table(df_7)

mycursor.execute("SELECT DISTINCT channels.channel_name, YEAR(video.Published_date) AS Published_year FROM channels JOIN playlist ON channels.channel_id=playlist.channel_id JOIN video ON playlist.playlist_id=video.playlist_id GROUP BY channels.channel_name,video.Published_date HAVING Published_year=2022")
query_8=mycursor.fetchall()
df_8=pd.DataFrame(query_8,columns=['Channel_name','Published_Year'])
st.table(df_8)

mycursor.execute("SELECT channel_name, ROUND(AVG(Duration),2) From channels JOIN playlist ON channels.channel_id=playlist.channel_id JOIN video ON playlist.playlist_id=video.playlist_id GROUP BY channels.channel_name")
query_9=mycursor.fetchall()
df_9=pd.DataFrame(query_9,columns=['channel_name','Avg_Duration'])
st.table(df_9)

mycursor.execute("SELECT channels.channel_name, video.Title, video.comment_count FROM channels JOIN playlist ON playlist.channel_id=channels.channel_id JOIN video ON video.playlist_id=playlist.playlist_id ORDER BY video.comment_count DESC LIMIT 15")
query_10=mycursor.fetchall()
df_10=pd.DataFrame(query_10, columns=['Channel_names','Video_names','Highest_Comments'])
st.table(df_10)



