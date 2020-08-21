#!/usr/bin/env python
# coding: utf-8

# # Extract, Transform, Load [[ETL]](https://www.stitchdata.com/etldatabase/etl-process/) with the [Youtube Data API](https://developers.google.com/youtube/v3/docs/videos/list?apix_params=%7B%22part%22%3A%5B%22snippet%2CcontentDetails%2Cstatistics%22%5D%2C%22id%22%3A%5B%22Ks-_Mh1QhMc%22%5D%7D)
# 
# ## *Abstract:*   
# *This notebook extracts all the video info from a specific youtube playlist then transformans and loads it into a postgre db*
# - You will need a YouTube Data API Key to continue. Obtain one [here](https://console.developers.google.com/apis/credentials?project=youtube-import-282315&folder=&organizationId=) 
# - Recommended soundtrack: [Earth Wind & Fire - I am](https://www.youtube.com/watch?v=6Z2xClustQo&list=PLyLqIRlItpe0SGOR9H66eTXiDSk2OEwDp)
# 
# 
# 
# ## Goals: 
# For git users, to follow along with the intext links in this index try out [nbviewer for this notebook](https://nbviewer.jupyter.org/github/jmhcodes/youtube/blob/master/important_videos.ipynb)
# >#### DB set up
# > Set up Postgres DB on your machine or online platform. [Acquire YouTubeAPI Key](https://rapidapi.com/blog/how-to-get-youtube-api-key/)
# - ***These two steps must be done before starting this notebook and are explained in my git***
# 
# > ### [Extract](#Extract_main)
# - [E1.](#E1) Write YouTube Data API query function to pull all videos in a playlist
#     - [Youtube API Code Examples](https://developers.google.com/youtube/v3/docs/videos/list?apix_params=%7B%22part%22%3A%5B%22snippet%22%5D%2C%22chart%22%3A%22mostPopular%22%2C%22regionCode%22%3A%22US%22%7D#try-it)
# - [E2.](#E2) Run playlists() function to return playlist.json
# - [E3.](#E3) Visualize the playlist.json, check it's keys and prep to transform the needed data
# 
# >### [Transform](#Transform_main)
# - [T1.](#T1) [pd.json_normalize()](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.json_normalize.html) is a very convenient method to flatten out json files and has many more kwargs that can be used
# - [T2.](#T2) Confirm our [pagination](https://developers.google.com/youtube/v3/guides/implementation/pagination) worked in the extract query by checking df length against total results of playlist json
# - [T3.](#T3) Begin transforming the DataFrame to exactly what we want to load
#  - [ ] [A.](#T3a) Create clickable a clickable links, take a break and check out a video
#  - [ ] [B.](#T3b) Write and use function to split column names into the desired format
#  - [ ] [C.](#T3c) Cast 'videopublishedat' as a DateTime object
# 
# >### [Load](#Load_main)
# - [L1.](#L1) Use psycopg2 to connect to our new Postgre DB
# - [L2.](#L2) Create a youtube_test DATABASE.
# - [L3.](#L3) Create playlist TABLE in the new youtube_test DATABASE
#    -  [ ] [A.](#L3a) We'll have to connect to the new youtube_test DATABASE
# - [L4.](#L4) Use sqlalchemy to upload our DataFrame to our new playlists table
# - [L5.](#L5a) Confirm our DataFrame uploaded to our desired location
#    - [ ] [A.](#L5a) Reconnect with psycopg2
#    - [ ] [B.](#L5b) Write function to get the column names of the table for our sql_df DataFrame
#    - [ ] [C.](#L5c) Query the playlists table to view your data

# In[1]:


from pandas import json_normalize
from datetime import datetime
import json
import pandas as pd
import os
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
import math
import webbrowser
import random


# <a id='Extract_main'></a>
# ### Extract
# *E1. Write YouTube Data API query function to pull all the videos in a playlist*
# 
# *E2. Run playlists() function to return playlist.json*
# 
# *E3. Visualize the playlist.json, check it's keys and prep to transform the needed data*
# 

# <a id='E1'></a>
# #### E1. Write YouTube Data API query function to pull all the videos in a playlist
# 
# > ###### Below is a Youtube API query with pagination to retrieve all videos in lists>50. [Youtube API Code Examples](https://developers.google.com/youtube/v3/docs/videos/list?apix_params=%7B%22part%22%3A%5B%22snippet%22%5D%2C%22chart%22%3A%22mostPopular%22%2C%22regionCode%22%3A%22US%22%7D#try-it)
# - by default the API only returns 50 records per page so you have to paginate to the next page for the rest or the results
# - the response['nextPageToken'] key is used in playlists() for pagination to pull all results after the first page
#  - if youre interested in learning more about how pagination works in the youtube API check it out [here](https://developers.google.com/youtube/v3/guides/implementation/pagination)
# 
# #### *important note: you need to paste your API key into the developerKey variable*

# In[2]:


# -*- coding: utf-8 -*-
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/guides/code_samples#python
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

def playlists(plid=None, pageToken = None):
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    developerKey = "AIzaSyBCX58yBlqo_qGcQw173Wql8Q7B4ryRTkg"
    
    
    #The following section can be changed to utilize OAuth and the client_secrets_file if wanted. We use the API key instead
    # Get credentials and create an API client
    #flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
    #    client_secrets_file, scopes)
    #credentials = flow.run_console()
   
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=developerKey)

    #request first page of playlist videos
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        maxResults=50,
        playlistId=plid, 
        pageToken = pageToken
    )
    response = request.execute()
    r_old_playlists = response.copy()
    
    #request the remaning pages of playlist videos via nextPageToken pagination
    while 'nextPageToken' in response:
        pageToken = response['nextPageToken']
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            maxResults=50,
            playlistId=plid, 
            pageToken = pageToken
        )
        response = request.execute()
        r_old_playlists['items'] = r_old_playlists['items'] + response['items']
               
    return r_old_playlists


# *E1. Write YouTube Data API query function to pull all the videos in a playlist*
# <a id='E2'></a>
# #### E2. Run playlists() function to return playlist.json

# In[3]:


#You'll need to click on the link below with a youtube account signed in and proceed to the unverified app 
#Soon itll be verified
#you need to find the playlist id for your favorite playlist and enter it here
yt_json = playlists(plid="PLFsQleAWXsj_4yDeebiIADdH5FMayBiJo")


# *E1. Write YouTube Data API query function to pull all the videos in a playlist*
# 
# *E2. Run playlists() function to return playlist.json*
# <a id='E3'></a>
# 
# **E3. Visualize the playlist.json, check it's keys and see prep to transform the needed data**
# 
# >### Let's take a look at the json and its keys
#  - Overall its a pretty clean json and should flatten nicely into a df for us to insert into postgre. 
#  - Check out the yt_json.keys()
#  > 1. yt_json['kind'] is the type of YouTube Data API object returned: youtube#playlistItemListResponse
#  > 2.  yt_json['etag'] is the etag of the playlist: v0T_wMbw983TuSWNE80dymxMv6c
#  > 3. yt_json['nextPageToken'] will have the next page of videos. The first page after the original is CDIQAA
#  > 4. yt_json['items'], this contains the video info we are interested in
#     - **The video info we want is in the 'items' key. It has loads of cool info like:**  <br>
#         - kind, etag, publishedAt, title, description, channelTitle, playlistId, resourceId
#  > 5. yt_json['pageInfo']has total # of videos and current # returned: {'totalResults': 311, 'resultsPerPage': 50}

# In[4]:


print("yt_json.json dict keys:")
i = 1
for key in yt_json.keys():
    print(' \t {}. {}'.format(i, key))
    i += 1

print("\t \t 1. yt_json['kind'] YouTube Data API object returned: {}".format(yt_json['kind']))
print("\t \t 2. yt_json['etag'] is the etag of the playlist {}".format(yt_json['etag']))
print("\t \t 3. yt_json['nextPageToken'] will have the next page of videos {}".format(yt_json['nextPageToken']))
print("\t \t 5. yt_json['pageInfo']has total # of videos and current # returned {}".format(yt_json['pageInfo']))
print('\033[1m'+ "\n \t \t 4. yt_json['items'], this contains the video info we are interested in:")

yt_json['items'][0]


# <a id='Transform_main'></a>
# ### Transform
# *T1. [pd.json_normalize()](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.json_normalize.html) is a very convenient method to flatten out json files and has many more kwargs that can be used*
# 
# *T2. Confirm our pagination worked in the extract query by checking df length against total results of playlist json*
# 
# *T3. Begin transforming the DataFrame to exactly what we want to load*

# <a id='T1'></a>
# 
# **T1. [pd.json_normalize()](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.json_normalize.html) is a very convenient method to flatten out json files and has many more kwargs that can be used**
#     - Here's a snippet of what our df will look like after we flatten the json out

# In[5]:


pd.json_normalize(yt_json['items'][0])


# *T1. [pd.json_normalize()](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.json_normalize.html) is a very convenient method to flatten out json files and has many more kwargs that can be used*
# 
# <a id='T2'></a>
# **T2. Confirm our pagination worked in the extract query by checking df length against total results of playlist json**
#     - yt_json['pageInfo']['totalResults'] is the total number of videos in the playlist
#     - if yt_json['pageInfo']['totalResults']==df.shape[0] then you're certain the json query populated all the videos from the playlist

# In[6]:


playlist_items = pd.json_normalize(yt_json['items'])
print("Number of videos returned: {}".format(playlist_items.shape[0]))
print("Number of rows in dataframe: {}".format(playlist_items.shape[0]))
print("All videos returned? {}".format(yt_json['pageInfo']['totalResults']==playlist_items.shape[0]) )


# *T1. [pd.json_normalize()](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.json_normalize.html) is a very convenient method to flatten out json files and has many more kwargs that can be used*
# 
# *T2. Confirm our pagination worked in the extract query by checking df length against total results of playlist json*
# 
# <a id='T3a'></a>
# **T3. Begin transforming the DataFrame to exactly what we want to load**
#  - [ ] **A. Create clickable a clickable links, take a break and check out a video**
#  - [ ] B. Write and use function to split column names into the desired format
#  - [ ] C. Cast 'videopublishedat' as a DateTime object

# In[7]:


#creates playlist url string to enter into browser for quick watchablity
playlist_items['pl_url'] = 'https://www.youtube.com/watch?v=' + playlist_items['contentDetails.videoId'] + '&list=' + playlist_items['snippet.playlistId'] + '&index=' + playlist_items['snippet.position'].astype(str)
webbrowser.open(playlist_items['pl_url'][random.randint(0,30)])
playlist_items['pl_url'][0:]


# *T1. [pd.json_normalize()](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.json_normalize.html) is a very convenient method to flatten out json files and has many more kwargs that can be used*
# 
# *T2. Confirm our pagination worked in the extract query by checking df length against total results of playlist json*
# 
# <a id='T3b'></a>
# **T3. Begin transforming the DataFrame to exactly what we want to load**
#  - [X] A. Create clickable a clickable links, take a break and check out a video
#  - [ ] **B. Write and use function to split column names into the desired format and rename columns**
#  - [ ] C. Cast 'videopublishedat' as a DateTime object

# **split_col_names()** will extract well formatted column names to import into sql

# In[8]:


def split_col_names(list_i):
    """
    Splits lists on '.' and returns the second parameter of the split. Due to the pd.json_normalize() and syntax:
        'contentDetails.videoId' returns 'videoId'
    
    If there is no '.' in string then return the first value:
        'kind' returns 'kind'

    list i: input list
    temp_list: returned list 
    """
    temp_list = []
    list_i = [i.split(".") for i in list_i]
    
    for i in list_i: 
        if len(i)>1:
            temp_list.append(i[1])
        else: 
            temp_list.append(i[0])
            
    temp_list = [i.lower() for i in temp_list]
    return temp_list


# In[9]:


#select the cols to import into the db
cols = ['snippet.title', 'snippet.description',
       'pl_url', 'snippet.thumbnails.high.url',
        'contentDetails.videoPublishedAt',
       'snippet.channelTitle', 'snippet.position',
        'snippet.resourceId.videoId', 'snippet.playlistId',
       'etag','id']

#rename columns and set data types
new_cols = split_col_names(cols)
final_playlist = playlist_items.rename(columns=dict(zip(cols, new_cols)))[new_cols]


# *T1. [pd.json_normalize()](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.json_normalize.html) is a very convenient method to flatten out json files and has many more kwargs that can be used*
# 
# *T2. Confirm our pagination worked in the extract query by checking df length against total results of playlist json*
# 
# <a id='T3c'></a>
# **T3. Begin transforming the DataFrame to exactly what we want to load**
#  - [X] A. Create clickable a clickable links, take a break and check out a video
#  - [X] B. Write and use function to split column names into the desired format and rename columns
#  - [ ] **C. Cast 'videopublishedat' as a DateTime object**

# In[10]:


final_playlist['videopublishedat'] = pd.to_datetime(final_playlist['videopublishedat'].str[:10] + ' ' 
                                              + final_playlist['videopublishedat'].str[11:-1])


# ###### Here's the final df we will be loading into postgre

# In[11]:


final_playlist.head()


# <a id='Load_main'></a>
# ### Load 
# *L1.  Use psycopg2 to connect to our new Postgre DB*
# 
# *L2. Create a youtube_test DATABASE.*
# 
# *L3. Create playlist TABLE in the new youtube_test DATABASE*
# - [ ] We'll have to connect to the new youtube_test DATABASE
# 
# *L4. Use sqlalchemy to upload our DataFrame to our new playlists table*
# 
# *L5. Confirm our DataFrame uploaded to our desired location*

# <a id='L1'></a>
# **L1.  Use psycopg2 to connect to our new Postgre DB**
# - To get familiar with psycopg2  we'll use it to connect to our new Postgre DB and create a few things
#     - psycopg2 is a standard package to connect from python to query postgres DBs

# In[12]:


# import the sql and connect libraries for psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql, connect
import psycopg2
import pandas as pd


# ###### Here you'll need to configure your db connection info to use psycopg2
# - db_name = Every install of postgres comes with a stock db called postgres that houses schema information of the whole db 
# - user = generally most people set up postgres as the default user on the initial install as well
# - host = the ip adress of the machine your db is located. if you db is on the same machine this query then "localhost" or "127.0.0.1" will work
#     - [There's no where like 127.0.0.1](https://www.lifewire.com/network-computer-special-ip-address-818385)
# - password = password of your db login 

# In[13]:


# create a global string for the PostgreSQL db name
db_name = "postgres"
user = "postgres"
host = "127.0.0.1" #192.168.1.206
password = "mypw"


# ###### It is common convention to call your connection conn or con like we do here connecting to our db listed above
#  - then we create a cur object from the conn.cursor() method 
#  - if your connection throws an exception it will print below, [happy debugging](https://www.postgresql.org/docs/12/errcodes-appendix.html)

# In[14]:


#Set up the connection string to your db
try:
    # declare a new PostgreSQL connection object
    conn = connect(
        dbname = db_name,
        user = user,
        host = host,
        password = password
    )

    # print the connection if successful
    print ("psycopg2 connection:\n", str(conn).split('; ')[1])
    cur = conn.cursor()

except Exception as err:
    print ("psycopg2 connect() ERROR:", err)
    conn = None


# *L1.  Use psycopg2 to connect to our new Postgre DB*
# 
# <a id='L2'></a>
# **L2. Create a youtube_test DATABASE**
# - Below we use our cur object method execute to create a youtube_test DATABASE and playlists TABLE
# - This table will store the our final_playlist DataFrame
# 
# 

# In[15]:


try: 
    # Connect to PostgreSQL DBMS
    conn = psycopg2.connect("user=postgres password='mypw'");
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);

    # Obtain a DB Cursor
    cursor          = con.cursor();
    name_Database   = "youtube_test";

    # Drop table statement
    sqlDropDatabase = "DROP DATABASE IF EXISTS " +name_Database+";" 
    
    # Create table statement
    sqlCreateDatabase = "create database "+name_Database+";"

    # Drop and Create a table in PostgreSQL database
    cursor.execute(sqlDropDatabase);
    cursor.execute(sqlCreateDatabase);

except Exception as err:
    print ("psycopg2 connect() ERROR:", err)

conn.close() #closes connection to database


# *L1.  Use psycopg2 to connect to our new Postgre DB*
# 
# *L2. Create a youtube_test DATABASE.*
# 
# <a id='L3'></a>
# **L3. Create playlist TABLE in the new youtube_test DATABASE**
# - [ ] **A. We'll have to connect to the new youtube_test DATABASE**

# In[16]:


db_name = 'youtube_test'
#Set up the connection string to your db
try:
    # declare a new PostgreSQL connection object
    conn = connect(
        dbname = db_name,
        user = user,
        host = host,
        password = password
    )

    # print the connection if successful
    print ("psycopg2 connection:\n", str(conn).split('; ')[1])
    cur = conn.cursor()

except Exception as err:
    print ("psycopg2 connect() ERROR:", err)
    conn = None


# *L1.  Use psycopg2 to connect to our new Postgre DB*
# 
# *L2. Create a youtube_test DATABASE.*
# 
# <a id='L3a'></a>
# **L3. Create playlist TABLE in the new youtube_test DATABASE**
# - A. [X] *We'll have to connect to the new youtube_test DATABASE*

# In[17]:


try: 
    cur.execute("""
    DROP TABLE IF EXISTS playlists;
    
    CREATE TABLE playlists (
        title TEXT, 
        description TEXT,
        pl_url TEXT,
        thumbnails TEXT,
        videopublishedat TIMESTAMP,
        channeltitle TEXT,
        position INTEGER,
        resourceid TEXT, 
        playlistid TEXT,
        etag TEXT,
        id TEXT,
        PRIMARY KEY (etag)
        );
    """)
    conn.commit() #without this .commit() method the table won't write to the DataBase

except Exception as err:
    print ("psycopg2 connect() ERROR:", err)   

conn.close()


# *L1.  Use psycopg2 to connect to our new Postgre DB*
# 
# *L2. Create a youtube_test DATABASE.*
# 
# *L3. Create playlist TABLE in the new youtube_test DATABASE*
# - [X] We'll have to connect to the new youtube_test DATABASE
# 
# <a id='L4'></a>
# **L4. Use sqlalchemy to upload our DataFrame to our new playlists table**
# - sqlalchemy is another common connection method used to read and write data to a db from python
#         - sqlalchemy really works like an [ORM](https://docs.sqlalchemy.org/en/13/core/engines.html) 

# In[18]:


table_name = 'playlists'
from sqlalchemy import create_engine
engine = create_engine("postgresql://postgres:mypw@127.0.0.1/youtube_test")
conn = engine.connect()
final_playlist.to_sql(name=table_name,con=conn,if_exists='append', index=False)
conn.close()


# *L1.  Use psycopg2 to connect to our new Postgre DB*
# 
# *L2. Create a youtube_test DATABASE.*
# 
# *L3. Create playlist TABLE in the new youtube_test DATABASE*
#    -  [X] *We'll have to connect to the new youtube_test DATABASE*
# 
# *L4. Use sqlalchemy to upload our DataFrame to our new playlists table*
# 
# <a id='L5a'></a>
# **L5. Confirm our DataFrame uploaded to our desired location**
#    - [ ] **A. Reconnect with psycopg2**
#    - [ ] B. Write function to get the column names of the table for our sql_df DataFrame
#         - get_cols is a utility function needed to find the column names from a postgres sql table
#    - [ ] C. query the playlists table to view your data

# In[19]:


#Set up the connection string to your db
try:
    # declare a new PostgreSQL connection object
    conn = connect(
        dbname = db_name,
        user = user,
        host = host,
        password = password
    )

    # print the connection if successful
    print ("psycopg2 connection:\n", str(conn).split('; ')[1])
    cur = conn.cursor()

except Exception as err:
    print ("psycopg2 connect() ERROR:", err)
    conn = None


# *L1.  Use psycopg2 to connect to our new Postgre DB*
# 
# *L2. Create a youtube_test DATABASE.*
# 
# *L3. Create playlist TABLE in the new youtube_test DATABASE*
#    -  [X] *We'll have to connect to the new youtube_test DATABASE*
# 
# *L4. Use sqlalchemy to upload our DataFrame to our new playlists table*
# 
# <a id='L5b'></a>
# L5. **Confirm our DataFrame uploaded to our desired location**
#    - [X] *A. Reconnect with psycopg2*
#    - [ ] **B. Write function to get the column names of the table for our sql_df DataFrame**
#         - get_cols is a utility function needed to find the column names from a postgres sql table
#    - [ ] C. query the playlists table to view your data

# In[20]:


def get_cols(table = None):
    """
    function that gets the column names from a PostgreSQL table
    
    table: input table to retrieve cols form
    columns: returns list table's cols
    """
    # declare an empty list for the column names
    columns = []

    # declare cursor objects from the connection    
    col_cursor = conn.cursor()

    # concatenate string for query to get column names
    # SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'some_table';
    col_names_str = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE "
    col_names_str += "table_name = '{}';".format( table )

    # print the SQL string
    print ("col names pull sql query:\n", col_names_str)
    
    #NOTE: It’s best to use the sql.SQL() and sql.Identifier() 
    #modules to build the SQL statement for you, 
    #instead of just concatenating the string yourself. 
    #Doing this can help prevent SQL injection attacks.
    try:
        sql_object = sql.SQL(
            # pass SQL statement to sql.SQL() method
            col_names_str
        ).format(
            # pass the identifier to the Identifier() method
            sql.Identifier( table )
        )
        
        # execute the SQL string to get list with col names in a tuple
        col_cursor.execute( sql_object )

        # get the tuple element from the liast
        col_names = ( col_cursor.fetchall() )

        # iterate list of tuples and grab first element
        for tup in col_names:

            # append the col name string to the list
            columns += [ tup[0] ]
           
        # close the cursor object to prevent memory leaks
        col_cursor.close()
        
        # print list of tuples with column names
        print ("col names:\n", columns)

    except Exception as err:
        print ("get_columns_names ERROR:", err)

    # return the list of column names
    return columns


# *L1.  Use psycopg2 to connect to our new Postgre DB*
# 
# *L2. Create a youtube_test DATABASE.*
# 
# *L3. Create playlist TABLE in the new youtube_test DATABASE*
#    -  [X] *We'll have to connect to the new youtube_test DATABASE*
# 
# *L4. Use sqlalchemy to upload our DataFrame to our new playlists table*
# 
# <a id='L5c'></a>
# L5. **Confirm our DataFrame uploaded to our desired location**
#    - [X] *A. Reconnect with psycopg2*
#    - [X] *B. Write function to get the column names of the table for our sql_df DataFrame*
#         - get_cols is a utility function needed to find the column names from a postgres sql table
#    - [ ] **C. query the playlists table to view your data**

# In[21]:


try: 
    cur.execute("""
    select * from playlists
    """)
    
    sql_return = cur.fetchall()
    
    sql_df = pd.DataFrame(sql_return, columns = get_cols('playlists'))

except Exception as err:
    print ("psycopg2 connect() ERROR:", err)


# In[22]:


sql_df

