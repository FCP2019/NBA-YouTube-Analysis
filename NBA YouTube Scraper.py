#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  3 13:51:54 2022

@author: chrispatton
"""

from googleapiclient.discovery import build
import pandas as pd
import sqlite3


#set up database
con = sqlite3.connect('nbayoutube2.db')

cur = con.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS channels 
            (Channel_name text, Subscribers real, Views real, NumVids real, playlist_id text)''')

cur.execute('''CREATE TABLE IF NOT EXISTS videos 
            (Channel text, Title text, DatePublished datetime, Length text, Views real, Likes real , Comments real)''')


#initialize API Key
api_key = 'AIzaSyBufbrL1vm--cEWRQfaJSg48hYYvZxFH94'


channel_ids = ['UCMfT9dr6xC_RIWoA9hI0meQ', # 1. Celtics
               'UCTenKHt0h3VjdMvRWP6Lbvw', # 2. Nets
               'UC0hb8f0OXHEzDrJDUq-YVVw', # 3. Knicks
               'UC5qJUyng_ezl0TVjVJFqtfQ', # 4. 76ers
               'UCYBFE432C2AmNRDGEXE4uVg', # 5. Raptors
               'UCvZi1jVVZ2yq0k5kkjzmuGw', # 6. Bulls
               'UCOdS-I1sYkKWhtTjMUWP_TA', # 7. Cavs
               'UCtcSBo9EzOtXHxiPhU6RN8A', # 8. Pistons
               'UCUQDCnAwU-35cOo8WCzg6zA', # 9. Pacers
               'UCRZDEVva3Z8h_Q0VetTgDUA', # 10. Bucks
               'UCl8hzdP5wVlhuzNG3WCJa1w', # 11. Nuggets
               'UCXWDN5NKVFgnPt25CMh98Cg', # 12. Wolves
               'UCpXdQhy6kb5CTD8hKlmOL3w', # 13. Thunder
               'UCXk66yyzXo7-2M1BMqLhltQ', # 14. Blazers
               'UCv9iSdeI9IzWfV8yTDsMYWA', # 15. Jazz
               'UCeYc_OjHs3QNxIjti2whKzg', # 16. Warriors
               'UCoK6pw3iIVF9WAWnQd3hj-g', # 17. Clippers
               'UC8CSt-oVqy8pUAoKSApTxQw', # 18. Lakers
               'UCLxlWVVHz2a8SdCfxzVXzQw', # 19. Suns
               'UCSgFigczGdNMilV1K23JgUQ', # 20. Kings
               'UCZywaCS_y9YOSSAC9z3dIeg', # 21. Mavs
               'UCVD7l69MVGFq_wzQvbk9HbQ', # 22. Rockets
               'UCCK5EpWKYrAmILfaZThCV-Q', # 23. Grizzlies
               'UCHvG7tf62PwI04ZRfoptRSw', # 24. Pelicans
               'UCEZHE-0CoHqeL1LGFa2EmQw', # 25. Spurs
               'UCpfcwELvR1wtcRJ0UxNXHYw', # 26. Hawks
               'UCPhFI5lIFnSggz7HgY9UuuQ', # 27. Hornets
               'UC8bZbiKoPNRi3taABIaFeBw', # 28. Heat
               'UCxHFH-yfbhUrsWY4prPx3oQ', # 29. Magic
               'UChvzoBPvORuNGtHTMzjUsIQ'  # 30. Wizards
              ]



youtube = build('youtube', 'v3', developerKey=api_key)


#Pull channel data from the above list via YouTube API. (From Tech TFQ Tutorial) 
def get_channel_stats(youtube, channel_ids):
    all_data = []
    request = youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=','.join(channel_ids))
    response = request.execute() 
    
    for i in range(len(response['items'])):
        datadict = dict(ChannelName = str(response['items'][i]['snippet']['title']),
        Subscribers = float(response['items'][i]['statistics']['subscriberCount']),
        Views = float(response['items'][i]['statistics']['viewCount']),
        NumVids = float(response['items'][i]['statistics']['videoCount']),
        PlaylistID = str(response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
        )
        all_data.append(datadict)
        
    return all_data

#Return list of video IDs. Here, we can pass this list as a parameter for the next function
#where we retrieve data for each video. 
def get_video_ids(youtube, playlist_ids):
    
    request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_ids,
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
                        playlistId = playlist_ids,
                        maxResults = 50,
                        pageToken = next_page_token)
            response = request.execute()
    
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            
            next_page_token = response.get('nextPageToken')
        
    return video_ids

# Retrieve engagement, viewership, length, date posted, and title for every video posted
#by every channel in the above "channel_ids" list
def get_video_details(youtube, video_ids):
    all_video_stats = []
    
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
                    part='snippet,contentDetails,statistics',
                    id=','.join(video_ids[i:i+50]))
        response = request.execute()
        try:
            for video in response['items']:
                datadict2 = dict(Channel = video['snippet']['channelTitle'],
                Title = video['snippet']['title'],
                Published_date = video['snippet']['publishedAt'],
                Length = video['contentDetails']['duration'],
                Views = video['statistics']['viewCount'],
                Likes = video['statistics']['likeCount'],
                Comments = video['statistics']['commentCount']
                )
                all_video_stats.append(datadict2)
            
        except:
            pass
        
    return all_video_stats

teams = get_channel_stats(youtube, channel_ids)


#Format list of dictionaries returned in above function as Pandas dataframe
#add 'channels' table to 'nbayoutube2' database
teamsdf = pd.DataFrame(teams)
teamsdf.to_sql(name='channels', con=con, if_exists='replace', index=False)
print(teamsdf.head())


#Return list of playlists from PlaylistID column in above dataframe
playlist_ids = list(teamsdf['PlaylistID'])
print (playlist_ids)


video_ids = []

#Build list of video IDs to retrieve video-level data
for x in range (len(playlist_ids)):
   video_ids.append(get_video_ids(youtube, playlist_ids[x]))



#Convert list of tuples to one large list
vids = []
for sublist in video_ids:
    for item in sublist:
       vids.append(item)



#Convert list of dictionaries to Pandas dataframe
#create table 'videos' in 'nbayoutube2' database and populated with data from dataframe
video_data = get_video_details(youtube, vids)
video_data_df = pd.DataFrame(video_data)
video_data_df.to_sql(name='videos', con=con, if_exists='replace', index=False)




