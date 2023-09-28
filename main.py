import streamlit as st
import data_scrapper
import migrate_sql
import store_mongo
from streamlit_option_menu import option_menu
import pandas as pd

with st.sidebar:
    selected = option_menu(
        menu_title="YOUTUBE DATA HARVESTING",
        options=["ABOUT PROJECT","SAMPLE DATA","STORING DATA","MIGRATING DATA","DATA ANALYSIS"],
        default_index=0,
    )

if selected == "STORING DATA":
    st.title("STORING DATA IN MONGO LAKE")
    
# getting youtube channel_id
    channel_id = st.text_input('Enter the Channel Id')
    collections = store_mongo.show_collections()
    search,store = st.columns(2)

    if search.button('SEARCH'):
        if channel_id == '':
            st.write("Please Enter the Channel_id (Refer ABOUT PROJECT to extract Channel ID)")
        else:
            channel_info = data_scrapper.scrape_channel_data(channel_id)
            if channel_info.get('Channel_Name',0):
                st.markdown(f"""## CHANNEL NAME : {channel_info['Channel_Name']}
                                Click STORE button to store {channel_info['Channel_Name']} data in Mongo Lake""")
            else:
                st.write(channel_info["Error"])

    if store.button('STORE'):
        if channel_id == '':
            st.write("Please Enter the Channel_id (Refer ABOUT PROJECT to extract Channel ID)")
        else:
            stored_info = store_mongo.store_data(channel_id)
            if stored_info.get("Error",0):
                st.write(stored_info["Error"])
            else:
                st.write(stored_info["stored_info"])
                collections = store_mongo.show_collections()

    st.write("### Channel Data's stored in Mongo Lake")
    st.table(pd.DataFrame(collections,columns=["Channel Name"]))

if selected == "MIGRATING DATA":

    st.title("DATA MIGRATION")
    
    selected_channel = st.selectbox("Select the Channel Name to Migrate/Delete the Data in Mongo Lake",store_mongo.show_collections())
    sql_channel_list = migrate_sql.channel_list_query()

    delete,migrate = st.columns(2)

    if delete.button('DELETE'):
        if selected_channel != None:
            store_mongo.delete(selected_channel)
            st.write(f"{selected_channel} Channel is deleted")
        else:
            st.write("Please Select the Channel")

    if migrate.button('MIGRATE'):
        migrate_info = migrate_sql.migrate_to_sql(selected_channel)
        st.write(migrate_info)
        sql_channel_list = migrate_sql.channel_list_query()

    st.write("### Click Truncate to delete all the data from MYSQL Database")
    if st.button("Truncate"):
        truncate_info = migrate_sql.truncate()
        st.write(truncate_info)
        sql_channel_list = migrate_sql.channel_list_query()

    st.write(pd.DataFrame(sql_channel_list,columns=["Channel Data in MYSQL Database"]))

if selected == "DATA ANALYSIS":

    st.title("ANALYSE THE DATA")
    
    if st.checkbox("What are the names of all the videos and their corresponding channels?",value=False):
        st.write(migrate_sql.first_query())

    if st.checkbox("Which channels have the most number of videos, and how many videos do they have?",value=False):
        st.write(migrate_sql.second_query())

    if st.checkbox("What are the top 10 most viewed videos and their respective channels?",value=False):
        st.write(migrate_sql.third_query())

    if st.checkbox("How many comments were made on each video, and what are their corresponding video names?",value=False):
        st.write(migrate_sql.fourth_query())

    if st.checkbox("Which videos have the highest number of likes, and what are their corresponding channel names?",value=False):
        st.write(migrate_sql.fifth_query())

    if st.checkbox("What is the total number of likes and dislikes for each video, and what are their corresponding video names?",value=False):
        st.write(migrate_sql.sixth_query())

    if st.checkbox("What is the total number of views for each channel, and what are their corresponding channel names?",value=False):
        st.write(migrate_sql.seventh_query())

    if st.checkbox("What are the names of all the channels that have published videos in the year 2022?",value=False):
        st.write(migrate_sql.eighth_query())

    if st.checkbox("What is the average duration of all videos in each channel, and what are their corresponding channel names?",value=False):
        st.write(migrate_sql.nighth_query())

    if st.checkbox("Which videos have the highest number of comments, and what are their corresponding channel names?",value=False):
        st.write(migrate_sql.tenth_query())

if selected == "ABOUT PROJECT":
    st.write("""
    # EXPLANATION ABOUT PROJECT
    1. YouTube Data Retrieval: The Streamlit application allows users to input a YouTube channel ID, leveraging the Google API to fetch essential channel data.

    2. Data Lake Storage: The application provides an option to store the collected YouTube data into a MongoDB database, functioning as a data lake. 

    3. Data Collection : Users can easily collect data from up to 10 different YouTube channels by clicking a button in the app.
            
    4. Migration : Furthermore, users have the option to select a specific channel and migrate its data from the data lake to a SQL database as structured tables.

    5. Comprehensive Analysis: Overall, the Streamlit application enables users to access and analyze YouTube channel data efficiently. """)

    st.write("""
    # HOW TO GET CHANNEL ID

    1. Visit the YouTube Channel: Open your web browser and navigate to the YouTube channel for which you want to find the channel ID.

    2. Right-Click on the Page: While on the channel's main page, right-click on any part of the page. A context menu will appear.

    3. Select "View Page Source": In the context menu, select the option that says "View Page Source." This will open a new tab or window displaying the HTML source code of the YouTube channel's page.""")

    st.image("./images/main.png")

    st.write("""
    4. Search for Channel ID: In the new tab with the HTML source code, use the keyboard shortcut Ctrl + F (or Command + F on Mac) to open the search function. Type "channelId" into the search box and press Enter.

    5. Locate the Channel ID: The search will highlight instances of "channelId" in the HTML source code.""")

    st.image("./images/view.png")

if selected == "SAMPLE DATA":
    st.write("## THIS PAGE PROVIDES COLLECTION OF CHANNEL NAME AND CHANNEL ID")
    st.table(pd.DataFrame( 
        {
        "Channel Name":["Irfans view","Sneholic","Tamil Voice over","Rahul M","Chloe ting","Black sheep","Food Impramation","Village cooking channel","A2D","Analyst adithya"],
        "Channel ID":["UCnjU1FHmao9YNfPzE039YTw","UCN9nKeOHC_cH2w9svgVypyA","UCTIuWYnWo-7CmYZqXD8WFRA","UC7beWKhXaZuRpD-oKQj852g","UCCgLoMYIyP0U56dEhEL1wXQ","UCzh5hQc_O3r3xjh9sXrM7-A","UCrRvwKQ49YIJ8jtToanksWQ","UCk3JZr7eS3pg5AGEvBdEvFg","UCvyZS6W6zMJCZBVzF-Ei6sw","UC61Y04JVLkByFRv1K3V-KGQ"]
        }))
