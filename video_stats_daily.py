#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]


# In[2]:


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


# In[3]:


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


# In[4]:


api_key = 'AIzaSyBCX58yBlqo_qGcQw173Wql8Q7B4ryRTkg'


# In[5]:


def video_stats():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "0"

    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "C:\\Users\\joshu\\Desktop\\Master SQL for DS\\Youtube\\client_secret_918685201498-5i7tmml50eod473lig1pk3ov14kb0c30.apps.googleusercontent.com.json"
    developerKey = "AIzaSyBCX58yBlqo_qGcQw173Wql8Q7B4ryRTkg"
    
    # Get credentials and create an API client
    #flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
    #    client_secrets_file, scopes)
    #credentials = flow.run_console()
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=developerKey)
    developerKey = "AIzaSyBCX58yBlqo_qGcQw173Wql8Q7B4ryRTkg"
    
    #request = youtube.videos().list(
    #        part="statistics",
    #        id=item)
    
    
    r_old_stats = []
    for item in yt_list: 
        request = youtube.videos().list(
            part="statistics",
            id=item)
        response = request.execute()
        r_old_stats = r_old_stats + response['items']

    return r_old_stats


# In[6]:


# create a global string for the PostgreSQL db name
db_name = "youtube_test"
user = "postgres"
host = "192.168.1.206"
password = "mypw"


# In[7]:


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


# In[8]:


try: 
    cur.execute("""
    select * from playlists
    """)
    
    sql_return = cur.fetchall()
    
    sql_df = pd.DataFrame(sql_return, columns = get_cols('playlists'))

except Exception as err:
    print ("psycopg2 connect() ERROR:", err)


# In[9]:


math.ceil(len(sql_df)/50)
yt_list = []
i = 0 
while i < math.ceil(len(sql_df)/50): 
    strlist = ",".join(sql_df['resourceid'][i*50:(i+1)*50])
    yt_list.append(strlist)
    i = i + 1


# In[10]:


video_stats_df = video_stats()


# In[11]:


final_video_stats = pd.json_normalize(video_stats_df)


# In[12]:


final_video_stats.drop(['kind','statistics.favoriteCount'],axis = 1, inplace = True)


# In[13]:


new_cols = split_col_names(final_video_stats.columns)


# In[14]:


final_video_stats = final_video_stats.rename(columns=dict(zip(final_video_stats.columns, new_cols)))[new_cols]


# In[15]:


final_video_stats['vid_date'] = datetime.now().strftime("%m-%d-%Y")
final_video_stats['vid_time'] = datetime.now().strftime("%H:%M:%S")
final_video_stats.rename(columns={'id':'resourceid'}, inplace=True)


# In[16]:


table_name = 'video_stats'
from sqlalchemy import create_engine
engine = create_engine("postgresql://postgres:mypw@192.168.1.206/youtube_test")
conn = engine.connect()
final_video_stats.to_sql(name=table_name,con=conn,if_exists='append', index=False)


# In[17]:


conn.close()

