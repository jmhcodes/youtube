#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pandas import json_normalize
from datetime import datetime
import json
import pandas as pd
import os
import google_auth_oauthlib.flow
import googleapiclient.discovery
from googleapiclient.discovery import build
import googleapiclient.errors
import math
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql, connect
import psycopg2
import pandas as pd

#import credentials (apiKey, database host address, client secret file) from your youtube_config.py
import sys
sys.path.insert(1, '../../')
from youtube_config import developerKey
from youtube_config import host
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]


# In[4]:


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


# In[5]:


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


# In[6]:


def video_stats(developerKey=developerKey):
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "0"

    api_service_name = "youtube"
    api_version = "v3"
    
    # Get credentials and create an API client

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=developerKey)
    developerKey = developerKey
    

    r_old_stats = []
    for item in yt_list: 
        request = youtube.videos().list(
            part="statistics",
            id=item)
        response = request.execute()
        r_old_stats = r_old_stats + response['items']

    return r_old_stats


# In[7]:


# create a global string to connect to PostgreSQL db
db_name = "youtube_test"
user = "postgres"
host = host
password = "mypw"


# In[8]:


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
    print ("psycopg2 connection:\n", str(conn).split(' ')[5:8], "host=host", str(conn).split(',')[1] )
    cur = conn.cursor()

except Exception as err:
    print ("psycopg2 connect() ERROR:", err)
    conn = None


# In[9]:


try: 
    cur.execute("""
    select * from playlists
    """)
    
    sql_return = cur.fetchall()
    
    sql_df = pd.DataFrame(sql_return, columns = get_cols('playlists'))

except Exception as err:
    print ("psycopg2 connect() ERROR:", err)


# In[10]:


#YouTube Data API only allows queries of 50 resources at a time
#split resourceid's in lists of 50 videos. 
math.ceil(len(sql_df)/50)
yt_list = []
i = 0 
while i < math.ceil(len(sql_df)/50): 
    strlist = ",".join(sql_df['resourceid'][i * 50: (i + 1) * 50])
    yt_list.append(strlist)
    i = i + 1


# In[11]:


video_stats_df = video_stats()


# In[12]:


final_video_stats = pd.json_normalize(video_stats_df)


# In[13]:


final_video_stats.drop(['kind','statistics.favoriteCount'],axis = 1, inplace = True)


# In[14]:


new_cols = split_col_names(final_video_stats.columns)


# In[15]:


final_video_stats = final_video_stats.rename(columns=dict(zip(final_video_stats.columns, new_cols)))[new_cols]


# In[16]:


final_video_stats['vid_date'] = datetime.now().strftime("%m-%d-%Y")
final_video_stats['vid_time'] = datetime.now().strftime("%H:%M:%S")
final_video_stats.rename(columns={'id':'resourceid'}, inplace=True)


# In[17]:


table_name = 'video_stats'
from sqlalchemy import create_engine
engine = create_engine("postgresql://postgres:mypw@192.168.1.206/youtube_test")
conn = engine.connect()


# In[18]:


final_video_stats.to_sql(name=table_name,con=conn,if_exists='append', index=False)
conn.close()


# In[ ]:




