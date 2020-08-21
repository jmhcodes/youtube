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

<!DOCTYPE html>
<html>
<head>
 <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
 <meta name="Author" content="Made by 'tree'">
 <meta name="GENERATOR" content="$Version: $ tree v1.8.0 (c) 1996 - 2018 by Steve Baker, Thomas Moore, Francesc Rocher, Florian Sesser, Kyosuke Tokoro $">
  <!-- 
  BODY { font-family : ariel, monospace, sans-serif; }
  P { font-weight: normal; font-family : ariel, monospace, sans-serif; color: black; background-color: transparent;}
  B { font-weight: normal; color: black; background-color: transparent;}
  A:visited { font-weight : normal; text-decoration : none; background-color : transparent; margin : 0px 0px 0px 0px; padding : 0px 0px 0px 0px; display: inline; }
  A:link    { font-weight : normal; text-decoration : none; margin : 0px 0px 0px 0px; padding : 0px 0px 0px 0px; display: inline; }
  A:hover   { color : #000000; font-weight : normal; text-decoration : underline; background-color : yellow; margin : 0px 0px 0px 0px; padding : 0px 0px 0px 0px; display: inline; }
  A:active  { color : #000000; font-weight: normal; background-color : transparent; margin : 0px 0px 0px 0px; padding : 0px 0px 0px 0px; display: inline; }
  .VERSION { font-size: small; font-family : arial, sans-serif; }
  .NORM  { color: black;  background-color: transparent;}
  .FIFO  { color: purple; background-color: transparent;}
  .CHAR  { color: yellow; background-color: transparent;}
  .DIR   { color: blue;   background-color: transparent;}
  .BLOCK { color: yellow; background-color: transparent;}
  .LINK  { color: aqua;   background-color: transparent;}
  .SOCK  { color: fuchsia;background-color: transparent;}
  .EXEC  { color: green;  background-color: transparent;}
  -->
</head>
<body>
	<h1>Directory Tree</h1><p>
	├── <a href=".//README.md">README.md</a> <br>
	├── <a href=".//requirements.txt">requirements.txt</a> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <i> <-The requirements file for reproducing the analysis environment</i> <br>
	├── <a href=".//notebooks/">notebooks</a> &nbsp;&nbsp;&nbsp; <i> <-Jupyter notebooks. Copies of etl.py files to dicuss techniques and digest code</i> <br>
	│   ├── <a href=".//notebooks/video_playlist_ETL.ipynb">video_playlist_ETL.ipynb</a> &nbsp;&nbsp;&nbsp; <i> <-Explains ETL import of unique videos/playlists from Youtube Data API to PostGres database</i> <br>
	│   └── <a href=".//notebooks/video_stats_hourly_ETL.ipynb">video_stats_hourly_ETL.ipynb</a> &nbsp;&nbsp;&nbsp; <i> <-Explains ETL import of hourly stats from the videos listed in the playlists pg table</i> <br>
	├── <a href=".//src/">src</a> &nbsp;&nbsp;&nbsp; <i> <-Source code used for this project</i> <br>
	│   ├── <a href=".//src/data/">data</a> &nbsp;&nbsp;&nbsp; <i> <-psycopg2 SQL scripts to generate data to visualize</i> <br>
	│   ├── <a href=".//src/etl/">etl</a> &nbsp;&nbsp;&nbsp; <i> <-psycopg2 SQL scripts to generate data to visualize</i> <br>
	│   │   ├── <a href=".//src/etl/video_playlist_ETL.py">video_playlist_ETL.py</a><br>
	│   │   └── <a href=".//src/etl/video_stats_hourly_ETL.py">video_stats_hourly_ETL.py</a><br>
	│   └── <a href=".//src/visulization/">visulization</a><br>
	│   &nbsp;&nbsp;&nbsp; └── <a href=".//src/visulization/data_viz.ipynb">data_viz.ipynb</a><br>
	├── <a href=".//youtube_config.py">youtube_config.py</a><br>
	└── <a href=".//youtube_env/">youtube_env</a><br>
	<br><br>
	</p>
	<p>

15 directories, 44 files
	<br><br>
	</p>
	<hr>
	<p class="VERSION">
		 tree v1.8.0 © 1996 - 2018 by Steve Baker and Thomas Moore <br>
		 HTML output hacked and copyleft © 1998 by Francesc Rocher <br>
		 JSON output hacked and copyleft © 2014 by Florian Sesser <br>
		 Charsets / OS/2 support © 2001 by Kyosuke Tokoro
	</p>
</body>
</html>





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

