# -*- encoding: utf-8 -*-
"""
Filename: sql_queries.py
Author: Mingxing Jin
Contact: Mingxing.Jin@bmw-brilliance.cn
Description: Design sql queries for data pipeline, and combine in list for iteration.
"""

import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES IF EXISTS
staging_events_table_drop = "DROP TABLE IF EXISTS staging_songs;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_events;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES FOR STAGING
staging_songs_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs integer,
    artist_id text,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location text,
    artist_name text,
    song_id text,
    title text,
    duration numeric,
    year integer
);
""")

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist text,
    auth text,
    firstname text,
    gender text,
    iteminsession integer,
    lastname text,
    lenth numeric,
    level text,
    location text,
    method text,
    page text,
    registration bigint,
    sessionid integer,
    song text,
    status text,
    ts bigint,
    useragent text,
    userid integer
);
""")


# CREATE TABLE FOR FACT AND DIMENSION
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id integer IDENTITY(0,1) PRIMARY KEY,
    start_time bigint NOT NULL REFERENCES time (start_time) SORTKEY,
    user_id integer NOT NULL REFERENCES users (user_id) DISTKEY,
    level text,
    song_id text NOT NULL REFERENCES songs (song_id),
    artist_id text NOT NULL REFERENCES artists (artist_id),
    session_id integer,
    location text,
    user_agent text
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id integer PRIMARY KEY DISTKEY SORTKEY,
    first_name text,
    last_name text,
    gender text,
    level text
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id text PRIMARY KEY DISTKEY SORTKEY,
    title text,
    artist_id text,
    year integer,
    duration numeric
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id text PRIMARY KEY SORTKEY,
    name text,
    location text,
    latitude numeric,
    longitude numeric
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time bigint PRIMARY KEY DISTKEY SORTKEY,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday integer
);
""")


# COPY SOURCE DATA FROM S3 BUCKETS TO STAGING TABLES
staging_events_copy = ("""
COPY staging_events FROM {}
iam_role {}
format as json {}
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
STATUPDATE ON;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""
COPY staging_songs FROM {}
iam_role {}
json 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


# INSERT DATA FROM STAGING TABLES INTO FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
    e.ts,
    e.userid,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId,
    s.artist_location,
    e.useragent
    FROM staging_events e
    JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
    e.userid,
    e.firstname,
    e.lastname,
    e.gender,
    e.level
    FROM staging_events e
    WHERE e.userid IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
    s.song_id,
    s.title,
    s.artist_id,
    s.year,
    s.duration  
    FROM staging_songs s
    WHERE s.song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
    s.artist_id,
    s.artist_name,
    s.artist_location,
    s.artist_latitude,
    s.artist_longitude
    FROM staging_songs s
    WHERE s.artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
    e.ts,
    EXTRACT(hour FROM '1970-01-01'::date + e.ts / 1000 * interval '1 second'),
    EXTRACT(day FROM '1970-01-01'::date + e.ts / 1000 * interval '1 second'),
    EXTRACT(week FROM '1970-01-01'::date + e.ts / 1000 * interval '1 second'),
    EXTRACT(month FROM '1970-01-01'::date + e.ts / 1000 * interval '1 second'),
    EXTRACT(year FROM '1970-01-01'::date + e.ts / 1000 * interval '1 second'),
    EXTRACT(weekday FROM '1970-01-01'::date + e.ts / 1000 * interval '1 second')
    FROM staging_events e
    WHERE e.ts IS NOT NULL;
""")


# VERIFY TABLES
verify_staging_events = ("""
SELECT COUNT(*) FROM staging_events;
""")
verify_staging_songs = ("""
SELECT COUNT(*) FROM staging_songs;
""")
verify_songplays = ("""
SELECT COUNT(*) FROM songplays;
""")
verify_users = ("""
SELECT COUNT(*) FROM users;
""")
verify_songs = ("""
SELECT COUNT(*) FROM songs;
""")
verify_artists = ("""
SELECT COUNT(*) FROM artists;
""")
verify_time = ("""
SELECT COUNT(*) FROM time;
""")




# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

verify_tables_queries = [verify_staging_events,verify_staging_songs,verify_songplays,verify_users,verify_songs,verify_artists,verify_time]