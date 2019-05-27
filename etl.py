import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.DataFrame(data=pd.read_json(filepath, lines=True))

    # insert song record
    song_cols = df[['song_id','title','artist_id','year','duration']].values
    song_data = song_cols[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_cols = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values
    artist_data = artist_cols[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.DataFrame(data=pd.read_json(filepath, lines=True))

    # filter by NextSong action
    dft = pd.DataFrame(data=df.loc[df['page'] == "NextSong"])

    # convert timestamp column to datetime
    t = pd.DataFrame(data=dft)
    t['time'] = pd.to_datetime(dft['ts'],unit='ms')
    ts_dict = {'time':pd.to_datetime(t['ts'],unit='ms'),'hour':t['time'].dt.hour,'day':t['time'].dt.day
           ,'wkofyr':t['time'].dt.weekofyear
           ,'month':t['time'].dt.month,'year':t['time'].dt.year,'wkday':t['time'].dt.weekday} 
    
    # insert time data records
    #time_data = pd.DataFrame(data=ts_dict)
    column_labels = ["time", "hours", "day", "wkofyr", "month","year","wkday"]
    time_df = pd.DataFrame(data=ts_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user = df[['userId','firstName','lastName','gender','level']].values
    user_df = pd.DataFrame(data=user)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in t.iterrows():
        
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
    
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
    for i, row in t.iterrows():
        songplay_data = (row['time'],row['userId'],row['level'], songid, artistid,row['sessionId'],row['location'],row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)




def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=pypost password=")
    cur = conn.cursor()

    process_data(cur, conn, filepath='/Users/gdx_home/Documents/DEND/project_template/data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='/Users/gdx_home/Documents/DEND/project_template/data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()