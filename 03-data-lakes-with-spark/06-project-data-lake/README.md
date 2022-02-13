# Data Lake with Spark
## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


## Project Datasets
* Song data: 's3://udacity-dend/song_data'  
* Log data: 's3://udacity-dend/log_data'  


#### Song Dataset
The first dataset contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.  
* song_data/A/B/C/TRABCEI128F424C983.json  
* song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.  
> {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}


#### Log Dataset
The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.  
* log_data/2018/11/2018-11-12-events.json   
* log_data/2018/11/2018-11-13-events.json     

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.  
> {"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}


## Schema for Song Play Analysis
Using the song and log datasets(json), I need to create a star schema optimized for queries on song play analysis. This includes the following tables.


### Fact Tables
* **songplays** - records in log data associated with song plays i.e. records with page NextSong (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, month, year)
### Dimension Tables
* **users** - users in the app (user_id, first_name, last_name, gender, level)   
* **songs** - songs in music database (song_id, title, artist_id, year, duration)  
* **artists** - artists in music database (artist_id, name, location, latitude, longitude)  
* **time** - timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday)


## Project Template
* **etl.py** reads data from S3, processes that data using Spark, and writes them back to S3  
* **etl.ipynb** test run for small data
* **dl.cfg** contains your AWS credentials    
* **README.md** provides discussion on your process and decisions    


## Project Steps
### Create user on AWS
* get the access key and secret key, and put it into dl.cfg


### Build ETL Pipeline
* Implement the logic in etl.py to extract data from s3 
* Transform data into fact and dimension tables
* Load tables into s3 in parquet format


### How to run
* Open terminal: python3 etl.py