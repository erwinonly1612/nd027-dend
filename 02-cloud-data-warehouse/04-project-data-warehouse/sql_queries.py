import configparser

# CONFIG
# Use configparser to read in the variables to connect with Amazon Redshift
# create a IAM user first, fill in dwh.cfg file

config = configparser.ConfigParser()
config.read('dwh.cfg')

SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES
# staging tables for reading data on s3
staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist                 varchar,
        auth                   varchar,
        firstName              varchar,
        gender                 varchar,
        itemInSession          integer,
        lastName               varchar,
        length                 float,
        level                  varchar,
        location               varchar,
        method                 varchar,
        page                   varchar,
        registration           float,
        sessionId              integer,
        song                   varchar,
        status                 integer,
        ts                     timestamp,
        userAgent              varchar,
        userId                 integer        
    )    
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs              integer,
        artist_id              varchar,
        artist_latitude        float,
        artist_longitude       float,
        artist_location        varchar,
        artist_name            varchar,
        song_id                varchar,
        title                  varchar,
        duration               float,
        year                   integer
    )
""")

# fact and dimension tables including songplays, songs, users, artists, times.
songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id            integer identity(1,1) primary key, 
        start_time             timestamp not null sortkey distkey,
        user_id                integer not null,
        level                  varchar,
        song_id                varchar not null,
        artist_id              varchar not null,
        session_id             integer,
        location               varchar,
        user_agent             varchar
    )
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id                integer not null sortkey primary key,
        first_name             varchar not null,
        last_name              varchar not null,
        gender                 varchar not null,
        level                  varchar not null
    )
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id                varchar not null sortkey primary key,
        title                  varchar not null,
        artist_id              varchar not null,
        year                   integer not null,
        duration               float
    )
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id              varchar not null sortkey primary key,
        name                   varchar not null,
        location               varchar,
        latitude               float,
        longitude              float
    )
""")

time_table_create = ("""
    CREATE TABLE times (
        start_time             timestamp not null distkey sortkey primary key,
        hour                   integer not null,
        day                    integer not null,
        week                   integer not null,
        month                  integer not null,
        year                   integer not null,
        weekday                varchar not null
    )

""")

# STAGING TABLES
# let COPY automatically load fields from the JSON file by specifying the 'auto' option, or you can specify a JSONPaths file that COPY uses to parse the JSON source data. 
# When moving large amounts of data from S3 staging area to Redshift, it is better to use the copy command instead of insert. The benefit of using the copy command is that the ingestion can be parallelized if the data is broken integero parts. Each part can be independently ingested by a slice in the cluster. 

staging_events_copy = ("""
    COPY staging_events FROM {bucket}
    credentials 'aws_iam_role={role}'
    region      'us-west-2'
    format      as JSON {path}
    timeformat  as 'epochmillisecs'
""").format(bucket=LOG_DATA, role=IAM_ROLE, path=LOG_PATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {bucket}
    credentials 'aws_iam_role={role}'
    region      'us-west-2'
    format      as JSON 'auto'
    timeformat  as 'epochmillisecs'
""").format(bucket=SONG_DATA, role=IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT 
        e.ts          as start_time, 
        e.userId      as user_id, 
        e.level       as level,
        s.song_id      as song_id,
        s.artist_id    as artist_id,
        e.sessionId   as session_id,
        e.location    as location,
        e.userAgent   as user_agent
    FROM staging_events e join staging_songs s
    ON e.song = s.title
    AND e.artist = s.artist_name
    AND e.page = 'NextSong'
    AND e.length = s.duration
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        e.userId       as user_id,
        e.firstName    as first_name,
        e.lastName     as last_name,
        e.gender       as gender,
        e.level        as level
    FROM staging_events e
    WHERE e.userId IS NOT NULL
    AND page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        s.song_id       as song_id,
        s.title        as title,
        s.artist_id     as artist_id,
        s.year         as year,
        s.duration     as duration
    FROM staging_songs s
    WHERE s.song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        s.artist_id         as artist_id,
        s.artist_name       as name,
        s.artist_location   as location,
        s.artist_latitude   as latitude,
        s.artist_longitude  as longitude
    FROM staging_songs s
    WHERE s.artist_id IS NOT NULL    
""")

time_table_insert = ("""
    INSERT INTO times (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        start_time                       as start_time,
        extract(hour from start_time)    as hour,
        extract(day from start_time)     as day,
        extract(week from start_time)    as week,
        extract(month from start_time)   as month,
        extract(year from start_time)    as year,
        extract(weekday from start_time) as weekday
    FROM songplays
""")

# for checking if the tables are created successfully
# count the number of rows
staging_events_count = ("""
    select count(*) from staging_events
""")

staging_songs_count = ("""
    select count(*) from staging_songs
""")

songplay_count = ("""
    select count(*) from songplays
""")

user_count = ("""
    select count(*) from users
""")

song_count = ("""
    select count(*) from songs
""")

artist_count = ("""
    select count(*) from artists
""")

time_count = ("""
    select count(*) from times
""")

# QUERY LISTS
# create_table_queries and drop_table_queries for create_tables.py
# copy_table_queries and insert_table_queries for eti.py
# count_rows_queries for analytics.py

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
count_rows_queries = [staging_events_count, staging_songs_count, songplay_count, user_count, song_count, artist_count, time_count]
