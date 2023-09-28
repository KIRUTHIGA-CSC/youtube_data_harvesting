from  pymongo import MongoClient
import data_scrapper

client = MongoClient('mongodb://localhost:27017/')
mydb = client['youtube_scrapping_project']

def store_lake(data):
    collection_name = data[0]["Channel_Name"]["Channel_Name"]
    mycol = mydb[collection_name]
    mycol.insert_many(data)
    return {"stored_info" : f"{collection_name} Channel Data is added to Mongo lake"}

def show_collections():
    return mydb.list_collection_names()

def delete(selected_channel):
    mydb[selected_channel].drop()

def store_data(channel_id):
    collections = show_collections()
    channel_data = data_scrapper.scrape_channel_data(channel_id)
    if channel_data.get("Channel_Name",0):
        collection_name = channel_data["Channel_Name"]
        if len(collections) < 10 and collection_name not in collections:
            video_data = data_scrapper.scrape_video_data(channel_id)
            stored_channel_info = store_lake(video_data)
            return stored_channel_info
        elif collection_name in collections:
            return {"stored_info":f"{collection_name} is already in MongoLake"}
        elif len(collections) == 10:
            return {"stored_info":"Maximum of 10 channel data can be stored"}
    else:
        return channel_data