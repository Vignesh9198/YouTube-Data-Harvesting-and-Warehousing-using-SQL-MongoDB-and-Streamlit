import streamlit as st
import googleapiclient.discovery
from pprint import pprint
import pandas as pd
import pymongo
import sqlite3

st.subheader('YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit', divider='rainbow')
st.subheader('Domain :blue[Social Media] :sunglasses:')


api = 'AIzaSyCi_FKyTGUBezocd4g2usVrmzeAbmeMOGI'
youtube = googleapiclient.discovery.build("youtube","v3",developerKey=api)
import pandas as pd

ch_id = [
    'UCnFrHLm2qQsMb9nND5SsCrA',
    'UCa9T0y46uK5FzJT-8tLBFeQ',
    'UCR4z8ccOWNoUThB4VAMNBTg',
    'UCFfFgeKVVhjXtQSXRj3e-Iw',
    'UC9ysV5ALsSZAnKMdXqyDsYw',
    'UCat88i6_rELqI_prwvjspRA',
    'UC5cY198GU1MQMIPJgMkCJ_Q',
    'UC9LjrPL1bLjJ2oIU3NSdcMQ',
    'UC_gXhnzeF5_XIFn4gx_bocg',
    'UCG0m9a2z1ziRm2YlaFuyU7A'
]

def Channel_Stats(youtube, ch_id):
    all_data = []

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=','.join(ch_id)
    )

    response = request.execute()

    for item in response['items']:
        z = {
            "Channel_id": item["id"],
            "Channel_name": item["snippet"]["title"],
            "Channel_description": item["snippet"]["description"],
            "Channel_subscribers": item["statistics"]["subscriberCount"],
            "Channel_views": item["statistics"]["viewCount"],
            "Channel_videos": item["statistics"]["videoCount"],
            "Channel_published": item["snippet"]["publishedAt"],
            "playlists_id": item["contentDetails"]["relatedPlaylists"]["uploads"]
        }
        all_data.append(z)

    return all_data

c = Channel_Stats(youtube, ch_id)
ChannelDetails = pd.DataFrame(c)


ChannelDetails["Channel_subscribers"]= ChannelDetails["Channel_subscribers"].astype(int)
ChannelDetails['Channel_views']= ChannelDetails['Channel_views'].astype(int)
ChannelDetails['Channel_videos']= ChannelDetails['Channel_videos'].astype(int)
ChannelDetails['Channel_published']= pd.to_datetime(ChannelDetails['Channel_published'])


def playlist_details(y): 
  request = youtube.playlists().list(
      part="snippet,contentDetails",
      channelId="UC_x5XG1OV2P6uZZ5FSM9Ttw",
      maxResults=25
    )
  response = request.execute()

  z = []
  for i in range(len(response["items"])):
    z.append(dict(playlist_title = response["items"][i]["snippet"]["title"],
                  playlist_description = response["items"][i]["snippet"]["description"],
                  playlist_published = response["items"][i]["snippet"]["publishedAt"],
                  playlist_views = response["items"][i]["contentDetails"]["itemCount"]))
  return z

y = playlist_details(ch_id)
playlistDetails =pd.DataFrame(y)

playlists_id = 'UUnFrHLm2qQsMb9nND5SsCrA'
def get_video_ids(youtube, playlist_id):
    
    request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_id,
                maxResults = 50)
    response = request.execute()
    

    video_ids = []
    
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
    
    next_page_token = response.get('nextPageToken')
    more_pages = True
    
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                         part='contentDetails',
                         playlistId = playlist_id,
                         maxResults = 50,
                         pageToken = next_page_token)
            response = request.execute()
    
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
    
            next_page_token = response.get('nextPageToken')
    
    return video_ids
video_id = get_video_ids(youtube, playlists_id)


def get_video_ids(youtube, playlist_id):
    video_ids = []

    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()

    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')

    while next_page_token:
        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')

    return video_ids

def get_video_details(youtube, video_ids):
    all_video_info = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {
                'snippet': ['channelId', 'title', 'description', 'publishedAt'],
                'statistics': ['viewCount', 'likeCount', 'commentCount']
            }

            video_info = {}
            video_info['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None

            all_video_info.append(video_info)

    return all_video_info

your_playlist_id = 'UUnFrHLm2qQsMb9nND5SsCrA'


video_ids = get_video_ids(youtube, your_playlist_id)


video_details = get_video_details(youtube, video_ids)


all_video_info = get_video_details(youtube,video_id)

videos_df = pd.DataFrame(all_video_info)

videos_df['publishedAt'] = pd.to_datetime(videos_df['publishedAt'])
videos_df['viewCount'] = videos_df['viewCount'].astype(int)
videos_df['likeCount'] = videos_df['likeCount'].astype(int)
videos_df['commentCount'] = videos_df['commentCount'].astype(int)

def fetch_comments(youtube, video_id):
    comments = []

    request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=100
    )
    response = request.execute()

    for item in response['items']:
        comment = item['snippet']['topLevelComment']['snippet']
        comments.append(comment)
    return comments

def fetch_comments(youtube, video_id):
    comments = []

    nextPageToken = None

    while True:

        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,
            pageToken=nextPageToken
        )
        response = request.execute()


        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            comments.append(comment)


        nextPageToken = response.get('nextPageToken')


        if nextPageToken is None:
            break

    return comments

comments = fetch_comments(youtube, videos_df['video_id'][5])

comments_df = pd.DataFrame(comments)

comments_df['authorDisplayName'] = comments_df['authorDisplayName'].astype(str)
comments_df['authorChannelUrl'] = comments_df['authorChannelUrl'].astype(str)
comments_df['authorProfileImageUrl'] = comments_df['authorProfileImageUrl'].astype(str)
comments_df['authorChannelId'] = comments_df['authorChannelId'].astype(str)

#Inserting data to mongodb.
client = pymongo.MongoClient("mongodb+srv://Vignesh_98:wekey1998@vignesh.ewc06kh.mongodb.net/?retryWrites=true&w=majority")

db = client["youtube_detils"]
col = db["ChannelDetails"]
documents = ChannelDetails.to_dict(orient='records')
col.insert_many(documents)
col1 = db["playlistDetails"]
col1.insert_many(y)
col2 = db["videos_details"]
col2.insert_many(all_video_info)
col3 = db["comments_details"]
col3.insert_many(comments)

#Creating table in sqlite3

conn = sqlite3.connect('youtube.db')
videos_df.to_sql('videos', conn, if_exists='append')
ChannelDetails.to_sql('ChannelDetails', conn, if_exists='append')
playlistDetails.to_sql('playlistDetails', conn, if_exists='append')
comments_df.to_sql('comments_details', conn, if_exists='append')

ChannelDetails = pd.read_sql_query("SELECT * FROM ChannelDetails", conn, index_col="index")
videos_df = pd.read_sql_query("SELECT * FROM videos", conn, index_col="index")
playlistDetails = pd.read_sql_query("SELECT * FROM playlistDetails", conn, index_col="index")
comments_df = pd.read_sql_query("SELECT * FROM comments_details", conn, index_col="index")

#(01) What are the names of all the videos and their corresponding channels

query1 = """
SELECT title, Channel_name
FROM videos
INNER JOIN ChannelDetails
ON Channel_id = ChannelDetails.Channel_id
"""
results1 = conn.execute(query1).fetchall()

videos_name_channel = []

for row in results1:
  title, channel_name = row
  videos_name_channel.append((title, channel_name))

videos_name_channel = pd.read_sql_query(query1, conn)



if st.button('Videos_Name_Channel'):
    for item in videos_name_channel:
        st.write(item)

#(02) What are the names of all the videos and their corresponding channels

query2= """
SELECT
    Channel_name AS ChannelName,
    MAX(Channel_videos) AS MaxVideos
FROM
    ChannelDetails;
"""
result2 = conn.execute(query2).fetchall()

most_num_videos_channel = []

for row in result2:
    channel_name, video_count = row
    most_num_videos_channel.append((channel_name, video_count))
  
most_num_videos_channel = pd.read_sql_query(query2, conn)


if st.button('Most_Num_Videos_Channel'):
        st.write(most_num_videos_channel)


#(03) Which videos have the highest number of views, and what are their corresponding channel names
query3 = """
SELECT DISTINCT title, viewCount
FROM videos
ORDER BY viewCount DESC
LIMIT 10;
"""

results3 = conn.execute(query3).fetchall()

top_10_videos = []

for row in results3:
    title, view_count = row
    top_10_videos.append((title, view_count))

top_10_videos = pd.read_sql_query(query3, conn)

if st.button('Top 10 Most Viewed Videos'):
    for item in top_10_videos :
        st.write(item)

#(04) Define the SQL query to retrieve the number of comments made on each video
query4 = """
SELECT title, commentCount
FROM videos
"""
results3 = conn.execute(query4).fetchall()

videos_comments_count = []

for row in results3:
    title, comment_count = row
    videos_comments_count.append((title, comment_count))

videos_comments_count = pd.read_sql_query(query4, conn)


if st.button('Videos_Cmments_Count '):
    for item in videos_comments_count :
        st.write(item)

#(05) Which videos have the highest number of likes, and what are their corresponding channel names

query5 = """
SELECT DISTINCT title AS Video_Title, Channel_name AS Channel_Name, likeCount AS Likes_Count
FROM videos AS v
JOIN ChannelDetails AS cd ON channelId = Channel_id
ORDER BY likeCount DESC
LIMIT 10;
"""
results5 = conn.execute(query5).fetchall()

highly_liked_videos = []

for row in results5:
    title, channel_name, likes_count = row
    highly_liked_videos.append((title, channel_name, likes_count))


highly_liked_videos = pd.read_sql_query(query5, conn)


if st.button('Highly_Liked_Videos'):
    st.write(highly_liked_videos)

#(07) What is the total number of views for each channel, and what are their corresponding channel names

query7 ="""
SELECT
    Channel_name,
    SUM(Channel_views) AS Total_Views
FROM
    ChannelDetails

GROUP BY
    Channel_name
ORDER BY Total_Views DESC
"""
results5 = conn.execute(query7).fetchall()

channel_views = []

for row in results5:
    channel_name, total_views = row
    channel_views.append((channel_name, total_views))

channel_views = pd.read_sql_query(query7, conn)


if st.button('channel_views'):
    st.write(channel_views)
#(08) What are the names of all the channels that have published videos in the year 2022

query = """
SELECT DISTINCT Channel_name
FROM videos AS v
INNER JOIN ChannelDetails AS c ON channelId = Channel_id
WHERE strftime('%Y', publishedAt) = '2022';
"""

results6 = conn.execute(query).fetchall()

published2022_channel = []

for row in results6:
    channel_name = row
    published2022_channel.append(channel_name)  

published2022_channel = pd.read_sql_query(query, conn)


if st.button('published2022_channel'):
    st.write(published2022_channel)



#(10) Which videos have the highest number of comments, and what are their corresponding channel names

query10 = '''
SELECT
    title AS Video_Title,
    ChannelDetails.Channel_name AS Channel_Name,
    MAX(commentCount) AS Max_Comment_Count
FROM
    videos
JOIN
    ChannelDetails ON videos.channelId = ChannelDetails.Channel_id
GROUP BY
    Video_Title, Channel_Name
ORDER BY
    Max_Comment_Count DESC
LIMIT 10;
'''

result7 = conn.execute(query10).fetchall()

highy_commented_videos = []

for row in result7:
    title, channel_name, comment_count = row
    highy_commented_videos.append((title, channel_name, comment_count))

highy_commented_videos = pd.read_sql_query(query10, conn)


if st.button('Highy_Commented_Videos'):
    for item in highy_commented_videos:
        st.write(item)


