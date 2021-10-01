# Data Modeling for Sparkify (Music-Streaming Startup)

## Introduction
![SparkifyLogo](img/SparkifyLogo.PNG)

A music-streaming startup called Sparkify wants to effectively analyze their exponentially-growing data related to songs and user activities on its platform. The Sparkify management team has assigned the task of analyzing the data to the analytics team, in which I belong to. 

To begin this journey, we decided to build a scalable data warehouse system, which integrates various datasets, such as songs and user-activities, from the Sparkify app.

![SparkifyDataWarehouse](img/SparkifyDataWarehouse.PNG)

## Sparkify ER Diagram
![SparkifyERDiagram](img/sparkifydb_erd.png)

## Project Datasets
* Song data: 'data/song_data'  
* Log data: 'data/log_data' 

#### Song Dataset
The first dataset contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.  
* song_data/A/B/C/TRABCEI128F424C983.json  
* song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.  
> {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

#### User-Activity Log Dataset
The log files in the dataset we'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.  
* log_data/2018/11/2018-11-12-events.json   
* log_data/2018/11/2018-11-13-events.json     

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.  
> {"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}


## Schema for Song Play Analysis
Using the song and log datasets(json), we'll create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
1.**songplays** - records in log data associated with song plays i.e. records with page NextSong (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
### Dimension Tables
2.**users** - users in the app (user_id, first_name, last_name, gender, level)   
3.**songs** - songs in music database (song_id, title, artist_id, year, duration)  
4.**artists** - artists in music database (artist_id, name, location, latitude, longitude)  
5.**time** - timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday)


## Project Instructions and Specifications
### Create Tables
* First step: run create_tables.py to create and connect to the Sparkify database, drops any tables if they exist, and creates the tables
* Run test.ipynb to confirm the creation of tables with the correct columns. Make sure to click "Restart kernel" to close the connection to the database after running this notebook.

### Build ETL Processes
* etl.ipynb reads and processes a single file from song_data and log_data and loads the data into the relevant tables. This notebook contains detailed instructions on the ETL process for each of the tables.
* Follow instructions and run each line in the etl.ipynb notebook to process the data for each table. 
* run test.ipynb to confirm that records were successfully inserted into each table. Remember to rerun create_tables.py to reset wer tables before each time we run this notebook.

### Build ETL Pipeline
* The script etl.py is derived from  the etl.ipynb above. It connects to the Sparkify database, extracts and processes the log_data and song_data, and loads data into the five tables.
* Follow instructions in the etl.py notebook to run the ETL processes for each table. 
* run test.ipynb to confirm that records were successfully inserted into each table. Remember to rerun create_tables.py to reset wer tables before each time we run this notebook.

### Final
* Run etl in console, and verify results: Python3 create_tables.py && python3 etl.py