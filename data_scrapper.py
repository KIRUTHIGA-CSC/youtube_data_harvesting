from googleapiclient.discovery import build
import isodate

# Please Enter the Generated API key
API_KEY = '**************************'

# Creating youtube service
youtube = build('youtube', 'v3', developerKey=API_KEY)

def scrape_channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,statistics",
        id= channel_id
    )
    response = request.execute()
    if "items" in response:
        return {
            "Channel_Name":  response['items'][0]['snippet']['title'],
            "Channel_Id": response['items'][0]['id'],
            "Subscription_Count": response['items'][0]['statistics']['subscriberCount'],
            "Channel_Views":response['items'][0]['statistics']['viewCount'],
            "Channel_Description": response['items'][0]['snippet']['description'],
        }
    else:
        return {"Error":"Please Enter Correct Channel ID"}
    
def scrape_playlist_data(channel_id):
    playlist_list = []
    request = youtube.playlists().list(
    part="contentDetails,snippet",
    channelId=channel_id,
    maxResults=50
    )
    response = request.execute()

    for i in range(len(response['items'])):
        playlist_list.append({"playlist_id" :response['items'][i]['id'],
                              "playlist_name": response['items'][i]['snippet']["title"]})
    
    next_page_token = response.get('nextPageToken')
    extra_pages = True

    while extra_pages:
        if next_page_token is None:
            extra_pages = False
        else:
            request = youtube.playlists().list(
            part="contentDetails,snippet",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
            response = request.execute()
            for i in range(len(response['items'])):
                playlist_list.append({"playlist_id" :response['items'][i]['id'],
                              "playlist_name": response['items'][i]['snippet']["title"]})
            next_page_token = response.get('nextPageToken')

    return playlist_list

def scrape_playlist_item_data(playlist_id):
    videos_list = []
    request = youtube.playlistItems().list(
        part="contentDetails",
        maxResults=50,
        playlistId= playlist_id
    )
    response = request.execute()
    for i in range(len(response['items'])):
        videos_list.append(response['items'][i]['contentDetails']['videoId'])

    next_page_token = response.get("nextPageToken")
    next_page = True

    while next_page:
        if next_page_token is None:
            next_page = False
        else:
            request = youtube.playlistItems().list(
                part="contentDetails",
                maxResults=50,
                playlistId= playlist_id,
                pageToken = next_page_token
            )
            response = request.execute()
            for i in range(len(response['items'])):
                videos_list.append(response['items'][i]['contentDetails']['videoId'])
            next_page_token = response.get("nextPageToken")
    videos_str = [','.join(videos_list[i:(i+50)]) for i in range(0,len(videos_list),50)]
    return videos_str

def scrape_video_comments(video_id):
    comments_dict = {}
    try:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId= video_id
        )
        response = request.execute()
        for i in range (len(response['items'])):
            comments_dict['Comment_Id_'+str(i+1)] =  {
                    "Comment_Id": response['items'][i]['id'],
                    "Comment_Text": response['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal'],
                    "Comment_Author":  response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "Comment_PublishedAt": response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
                }
    except Exception as e:
        return comments_dict
    return comments_dict

def scrape_video_data(channel_id):
    final_json = []
    playlist_list = scrape_playlist_data(channel_id)
    channel_data = scrape_channel_data(channel_id)

    for playlist_id in playlist_list:
        videos_dict = {}
        channel_dict = {}
        video_no = 1
        video_str = scrape_playlist_item_data(playlist_id["playlist_id"])
        channel_dict.update(channel_data)
        channel_dict.update({"Playlist_Id" : playlist_id["playlist_id"]})
        channel_dict.update({"Playlist_Name" : playlist_id["playlist_name"]})
        videos_dict["Channel_Name"] = channel_dict

        for each_batch in video_str:
            request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id = each_batch
            )
            response = request.execute()
            
            for i in range(len(response['items'])):
                videos_dict['Video_Id_'+str(video_no)] = {
                    "Video_Id": response['items'][i]['id'] ,
                    "Video_Name": response['items'][i]['snippet'].get('title',''),
                    "Video_Description": response['items'][i]['snippet'].get('description',''),
                    "Tags": response['items'][i]['snippet'].get('tags',[]),
                    "PublishedAt": response['items'][i]['snippet'].get('publishedAt',0),
                    "View_Count": response['items'][i]['statistics'].get('viewCount',0),
                    "Like_Count": response['items'][i]['statistics'].get('likeCount',0),
                    "Dislike_Count": 1,
                    "Favorite_Count": response['items'][i]['statistics'].get('favoriteCount',0),
                    "Comment_Count": response['items'][i]['statistics'].get('commentCount',0),
                    "Duration": isodate.parse_duration(response['items'][i]['contentDetails']['duration']).total_seconds(),
                    "Thumbnail": response['items'][i]['snippet']['thumbnails']['default'].get('url',''),
                    "Caption_Status": response['items'][i]['contentDetails'].get('caption',''),
                    'Comments': scrape_video_comments(response['items'][i]['id']) if response['items'][i]['statistics'].get('commentCount',0) !=0 else {}
                    }
                video_no = video_no+1
        final_json.append(videos_dict)
    return final_json






