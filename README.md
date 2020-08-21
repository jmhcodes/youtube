# youtube_analytics
This repo is in development

## Goal 
*Pull relevant marketing analytics statistics from the YouTube, Twitch, Twitter, Instagram APIs to benefit independent YouTuber's and video game releases*

  ### Completed
  - Built ETL to store playlist in postgreDB from Youtube Data API
  - Built ETL to pull daily channel, video, and playlist statistics (such as subs, views, likes, plays)

  ### Currently
  - Collecting YouTube daily statistics 

  ### Up next 
  - Build Twitter ETL 
  - Track a couple friends YouTube channel uploads and stats
  - Track my favorite YouTubers channel uploads and stats
  
  
    ### On the board
    - Build Twitch ETL 
    - Build Instagram ETL 
    - Pull for like video titles and see how far down the list yours lies
    - Correlate how Twitch/Twitter/Instagram reach affects Youtube Plays

├── README.md
├── requirements.txt                    <-The requirements file for reproducing the analysis environment, e.g.
│                                          generated with `pip freeze > requirements.txt`
├── notebooks                           <- Jupyter notebooks. Copies of etl.py files to dicuss techniques and digest code
│   ├── video_playlist_ETL.ipynb        <- Explains ETL import of unique videos/playlists from Youtube Data API to PostGres database
│   └── video_stats_hourly_ETL.ipynb    <- Explains ETL import of hourly stats from the videos listed in the playlists pg table
├── src                                 <- Source code used for this project
│   ├── data                            <- psycopg2 SQL scripts to generate data to visualize
│   ├── etl                             <- folder of ETL scripts decribed in the notebooks
│   │   ├── video_playlist_ETL.py
│   │   └── video_stats_hourly_ETL.py
│   └── visulization                    <- visualizations of hourly stats and analytics on key times to upload
│       └── data_viz.ipynb
├── youtube_config.py                   <- config file with YouTube Data API credentials and Postgres address
└── youtube_env                         <- virtual env used to run all scripts

