import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData

def process_song_file(cur, filepath):
    '''
    Process song json file in song_data filepath and then insert the song json data into both artist_table and song_table in the sparkifydb database.
    
    Args:
        cur (cursor): cursor() in psycopg2 to allow Python code to execute PostgreSQL command in a database session.    
        filepath (str): directory of the song json files
    
    Returns:
        None
    '''
    # open song file
    df = pd.read_json(filepath, lines= True)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)   
    
    
def process_log_file(cur, filepath):
    '''
    Process log json file in log_data filepath and then insert the log json data into both songplay_table in the sparkifydb database.
    
    Args:
        cur (cursor): cursor() in psycopg2 to allow Python code to execute PostgreSQL command in a database session.    
        filepath (str): directory of the log json files
    
    Returns:
        None
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = list(zip(df['ts'], df['ts'].dt.hour, df['ts'].dt.day, df['ts'].dt.week, df['ts'].dt.month, df['ts'].dt.year))
    column_labels = list(['timestamp','hour', 'day', 'week of year', 'month', 'year'])
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    process song_data and log_data in filepath.
    
    Args:
        cur (cursor): cursor() in psycopg2 to allow Python code to execute PostgreSQL command in a database session.    
        conn (connection): connection in psycopg2 to handle the connection to a PostgreSQL database instance.
        filepath (str): directory of the song.json files.
        func (): functions to process log data and json data.
    
    Returns:
        None
    '''
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
        connect to sparkifydb database and then process song data and log data
    '''
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()
    
    # create ER diagram of sparkifydb. Uncomment to regenerate the ER Diagram.
    #graph = create_schema_graph(metadata=MetaData('postgresql://student:student@127.0.0.1/sparkifydb'))
    #graph.write_png('img/sparkifydb_erd.png')

if __name__ == "__main__":
    main()