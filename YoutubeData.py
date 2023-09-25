import streamlit as st
import googleapiclient.discovery
from pprint import pprint
import pandas as pd
import pymongo
import sqlite3

st.subheader('YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit', divider='rainbow')
st.subheader('Domain :blue[Social Media] :sunglasses:')


api = 'AIzaSyCi_FKyTGUBezocd4g2usVrmzeAbmeMOGI'
ch_id = 'UCnFrHLm2qQsMb9nND5SsCrA'
youtube = googleapiclient.discovery.build("youtube","v3",developerKey=api)

def Channel_Stats(ch_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=ch_id)

    response = request.execute()

    z = dict(Channel_id = response["items"][0]["id"],
              Channel_name = response["items"][0]["snippet"]["title"],
              Channel_description = response["items"][0]["snippet"]["description"],
              Channel_subscribers = response["items"][0]["statistics"]["subscriberCount"],
              Channel_views = response["items"][0]["statistics"]["viewCount"],
              Channel_videos = response["items"][0]["statistics"]["videoCount"],
              Channel_published = response["items"][0]["snippet"]["publishedAt"],
              playlists_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"])
    return z

c = Channel_Stats(ch_id)

ChannelDetails = pd.DataFrame({"Channel_id" : [c["Channel_id"]],
                              "Channel_name" : [c["Channel_name"]],
                              "Channel_description" : [c["Channel_description"]],
                              "Channel_subscribers" : [c["Channel_subscribers"]],
                              "Channel_views" : [c["Channel_views"]],
                              "Channel_videos" : [c["Channel_videos"]],
                              "Channel_published" : [c["Channel_published"]],
                              "playlists_id" : [c["playlists_id"]]
                               })


ChannelDetails["Channel_subscribers"]= ChannelDetails["Channel_subscribers"].astype(int)
ChannelDetails['Channel_views']= ChannelDetails['Channel_views'].astype(int)
ChannelDetails['Channel_videos']= ChannelDetails['Channel_videos'].astype(int)
ChannelDetails['Channel_published']= pd.to_datetime(ChannelDetails['Channel_published'])

if ch_id and st.button('Channel_Details'):
   a = Channel_Stats(ch_id)
   st.write(a)

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


video_ids = []

request = youtube.playlistItems().list(
    part="snippet,contentDetails",
    playlistId=playlists_id,
    maxResults=50
  )
response = request.execute()

for item in response['items']:
    video_ids.append(item['contentDetails']['videoId'])

n_p_t = response.get('nextPageToken')
while n_p_t:
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlists_id,
        maxResults=50,
        pageToken=n_p_t
      )
    response = request.execute()
    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])
    n_p_t = response.get('nextPageToken')

print("Number of Videos : ",len(video_ids))

def get_video_details(youtube,video_id):
  all_video_info = []

  for i in range(0,len(video_ids),50):
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=','.join(video_ids[i:i+50])
      )
  response = request.execute()

  for video in response['items']:
    stats_to_keep = {'snippet' : ['title', 'description','publishedAt'],'statistics' : ['viewCount','likeCount','commentCount']}

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

comments = fetch_comments(youtube, videos_df['video_id'][0])

comments_df = pd.DataFrame(comments)

comments_df['authorDisplayName'] = comments_df['authorDisplayName'].astype(str)
comments_df['authorChannelUrl'] = comments_df['authorChannelUrl'].astype(str)
comments_df['authorProfileImageUrl'] = comments_df['authorProfileImageUrl'].astype(str)
comments_df['authorChannelId'] = comments_df['authorChannelId'].astype(str)

#Inserting data to mongodb.
client = pymongo.MongoClient("mongodb+srv://Vignesh_98:wekey1998@vignesh.ewc06kh.mongodb.net/?retryWrites=true&w=majority")

db = client["youtube_detils"]
col = db["ChannelDetails"]
col.insert_one(c)
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

# What are the names of all the videos and their corresponding channels?
query = """
SELECT title, Channel_name
FROM videos
INNER JOIN ChannelDetails
ON Channel_id = ChannelDetails.Channel_id
LIMIT 50  -- Add LIMIT clause to limit the results to 50 rows
"""

results = conn.execute(query).fetchall()

video_info = []


for row in results:
    title, channel_name = row
    video_info.append(f"Video: {title}, '======>',Channel_name: {channel_name}")


if st.button('Display Video Info'):
    for item in video_info:
        st.write(item)

# Which channels have the most number of videos, and how many videos do they have?


query1 = """select Channel_name,Channel_videos
from ChannelDetails
limit 1
"""
results1 = conn.execute(query1).fetchall()

top_ch_vi = [] 

for row in results1:
    channel_name,channel_videos = row
    top_ch_vi.append(f"Channel_name: {channel_name},'======>>',Channel_videos: {channel_videos}")

if st.button('Top channel_videos'):
        st.write(top_ch_vi)

#What are the top 10 most viewed videos and their respective channels?

query2 = """
SELECT title, viewCount, Channel_name
FROM videos
INNER JOIN ChannelDetails
ON Channel_id = ChannelDetails.Channel_id
ORDER BY viewCount DESC
LIMIT 10
"""


results2 = conn.execute(query2).fetchall()

top_viewed_videos = []

for row in results2:
    title, viewsCount, channel_name = row
    top_viewed_videos.append(f"Video Title: {title},'=====>>', Views: {viewsCount},'=====>>', Channel Name: {channel_name}")

if st.button('Top 10 Most Viewed Videos'):
    for item in top_viewed_videos:
        st.write(item)

# How many comments were made on each video, and what are their corresponding video names?

query3 = """
SELECT title, commentCount
FROM videos
"""
results3 = conn.execute(query3).fetchall()

comment_count = []

for row in results3:
    title, commentCount = row
    comment_count.append(f"Video Title: {title},'======>>', Comments: {commentCount}")

if st.button('Comment_count'):
    for item in comment_count:
        st.write(item)

# Which videos have the highest number of likes, and what are their corresponding channel names?

query4 = """
SELECT title,likeCount, Channel_name
FROM videos
INNER JOIN ChannelDetails
ON Channel_id = ChannelDetails.Channel_id
ORDER BY likeCount DESC
LIMIT 10
"""
results4 = conn.execute(query4).fetchall()

like_count = []

for row in results4:
    title,likeCount, channel_name = row
    like_count.append(f"video_title: {title},'======>>Channel Name: {channel_name},'======>>', Likes: {likeCount}")

if st.button('Top_Like_count'):
    st.write(like_count)

# What is the total number of likes and dislikes for each video, and what are their corresponding video names?

query5 = """
SELECT title,likeCount
FROM videos
ORDER BY likeCount DESC
LIMIT 10
"""
results4 = conn.execute(query5).fetchall()

video_likes = []

for row in results4:
    title,likeCount = row
    video_likes.append(f"video_title: {title},'======>>', Likes: {likeCount}")

if st.button('Videos_Most_likes'):
    st.write(video_likes)

# What is the total number of views for each channel, and what are their corresponding channel names?


query6 = """ SELECT Channel_name , Channel_views
FROM ChannelDetails
ORDER BY Channel_views DESC
LIMIT 1
"""
results6 = conn.execute(query6).fetchall()

Top_Channel_views = []

for row in results6:
  channel_name,channel_views = row
  Top_Channel_views.append(f"Channel_name: {channel_name},'======>>'Channel_views: {channel_views}")

if st.button('Top_Channel_Views'):
    st.write(Top_Channel_views)

#What are the names of all the channels that have published videos in the year 2020?

query7 = """
SELECT title, publishedAt
FROM videos
limit 20
"""

results7 = conn.execute(query7).fetchall()

published_videos_2020 = []

for row in results7:
    title, publishedAt = row
    published_videos_2020.append(f"VIDEO TITLE: {title}, PUBLISHED AT: {publishedAt}")

if st.button('Show Published Videos in 2020'):
    for item in published_videos_2020:
        st.write(item)
        

# Which videos have the highest number of comments, and what are their corresponding channel names?

query8 = """
SELECT videos.title,commentCount, ChannelDetails.Channel_name
FROM videos
INNER JOIN ChannelDetails ON Channel_id = ChannelDetails.Channel_id
ORDER BY commentCount DESC
Limit 1
"""

results8 = conn.execute(query8).fetchall()

top_commented_videos = []

for row in results8:
    title, commentCount, channel_name = row
    top_commented_videos.append(f"Video Title: {title}, '=====>>', Comments: {commentCount}, '=====>>', Channel Name: {channel_name}")

if st.button('Top Commented Videos'):
    for item in top_commented_videos:
        st.write(item)

