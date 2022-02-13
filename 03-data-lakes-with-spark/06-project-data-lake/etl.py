import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date
import pyspark.sql.functions as F

# read AWS key id and secret key
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create spark session with hadoop-aws package
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    read song data from s3 and then create the songs_table and artists_table. load them back to s3.
    
    parameters:
    spark: spark session
    input_data: path of song data
    output_data: path of output table
    
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    #song_data = "s3a://udacity-dend/song_data/A/A/A/*.json"
    #song_data = input_data + "song_data/A/B/C/TRABCEI128F424C983.json"
    
    # create song table schema
    songSchema = R([
    Fld("num_songs",Int()),
    Fld("artist_id",Str()),
    Fld("artist_latitude",Dbl()),
    Fld("artist_longitude",Dbl()),
    Fld("artist_location",Str()),
    Fld("artist_name",Str()),  
    Fld("title",Str()),  
    Fld("duration",Dbl()),  
    Fld("year",Int()),  
    ])

    # read song data file
    df = spark.read.json(song_data, schema=songSchema)
    
    # extract columns to create songs table, drop if year and artist_id are missing and year should not equal to 0
    song_field = ["title", "duration", "year", "artist_id"]
    
    print("df:",df.head())
    
    song_field = ["title", "duration", "year", "artist_id"]
    songs_table = df.select(song_field).dropDuplicates().withColumn("song_id",F.monotonically_increasing_id()).filter(~col("year").isin([0])&col("year").isNotNull()&col("artist_id").isNotNull())
    
    #print("songs_table_head:",songs_table.head())
    #print("songs_table_count:",songs_table.count())    
    
    # extract columns to create artists table, drop if artist_id and name containing any null values
    artist_field = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = df.select(artist_field).dropDuplicates().dropna(subset=["artist_id","artist_name"])
 
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs/", mode="overwrite")
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    read log data from s3 and then create the songplays_table, time_table and users_table. load them back to s3.

    parameters:
    spark: spark session
    input_data: path of song data
    output_data: path of output table

    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    #log_data = "s3a://udacity-dend/log_data/2018/11/*.json"
    #log_data = input_data + "log_data/2018/11/2018-11-12-events.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table, drop if rows containing any null values
    user_field = [" userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(user_field).dropDuplicates().dropna(how = "any")

    # write users table to parquet files
    users_table.write.parquet(output_data + "users/", mode="overwrite")

    # create datetime column from original timestamp column
    df = df.withColumn("start_time", F.to_timestamp(col("ts") / 1000))

    # extract columns to create time table
    time_table = df.select("start_time").dropDuplicates()\
    .withColumn("hour", hour("start_time")).withColumn("day", dayofmonth("start_time"))\
    .withColumn("week", weekofyear("start_time")).withColumn("month", month("start_time"))\
    .withColumn("year", year("start_time")).withColumn("weekday", date_format("start_time",\
    "E")).dropna(how="any")    

    df = df.withColumn("hour", hour("start_time"))
    df = df.withColumn("monthfordata", month("start_time"))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time/", mode="overwrite")

    # read in song and artist table to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/*/*/*")
    artist_df = spark.read.parquet(output_data + "artists/*")

    # extract columns from joined song and log datasets to create songplays table 
    # get artist_id and song_id from songs_table 
    # get location from artists_table 
    # get year and month from time_table
    #print(song_df.show())
    Join_song = df.join(song_df, ((song_df.title == df.song) & (song_df.duration == df.length)))
    artists_songs_logs = Join_song.join(artist_df, (Join_song.artist == artist_df.artist_name))
    songplays = artists_songs_logs.join(time_table, (artists_songs_logs.start_time == time_table.start_time), 'left').drop(artists_songs_logs.start_time)
    #print(songplays.show())    
    # extract columns to create songplays table
    songplays_field = ["start_time","year","monthfordata", "userId as user_id", "level", "song_id", "artist_id", "sessionid as\
    session_id", "artist_location as location", "userAgent as user_agent"]

    songplays_table = songplays.selectExpr(songplays_field).dropDuplicates()\
    .dropna(subset=["user_id","artist_id", "song_id","start_time"])\
    .withColumn("songplay_id",F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "monthfordata").parquet(output_data + "songplays/", mode="overwrite")
    
def main():
    """
    Extract songs and events data from S3, Transform it into fact and dimensional tables format, and Load it back to S3 in Parquet format
    
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dend-test/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
