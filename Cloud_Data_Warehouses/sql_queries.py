import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplay;"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists song"
artist_table_drop = "drop table if exists artist"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events
(
artist text, 
auth text, 
first_name text, 
gender text, 
item_in_session int, 
last_name text, 
length float, 
level text, 
location text, 
method text, 
page text, 
registration float, 
session_id int, 
song text, 
status int, 
ts bigint, 
user_agent text, 
user_id text
)
;
""")

staging_songs_table_create = ("""
create table if not exists staging_songs
(
song_id text,
title text,
duration float,
year int,
artist_id text,
artist_name text,
artist_latitude float,
artist_longitude float,
artist_location text,
num_songs int
)
;
""")

songplay_table_create = ("""
create table if not exists songplay 
(
songplay_id int identity (0,1) PRIMARY KEY,
start_time timestamp NOT NULL, 
user_id text NOT NULL, 
level text, 
song_id text NOT NULL,
artist_id text NOT NULL, 
session_id int, 
location text,
user_agent text
)
;
""")

user_table_create = ("""
create table if not exists users
(
user_id text PRIMARY KEY NOT NULL,
first_name text, 
last_name text, 
gender text, 
level text
)
;
""")

song_table_create = ("""
create table if not exists song
(
song_id text PRIMARY KEY NOT NULL, 
title text,
artist_id text, 
year int, 
duration float
)
;
""")

artist_table_create = ("""
create table if not exists artist
(
artist_id text PRIMARY KEY NOT NULL,
name text, 
location text, 
latitude float, 
longitude float
)
;
""")

time_table_create = ("""
create table if not exists time
(
start_time timestamp PRIMARY KEY NOT NULL,
hour int, 
day int, 
week int, 
month int, 
year int,
weekday int
)
;
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events 
from {}
iam_role {}
json {}
region {}
;
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
    config['CLUSTER']['REGION']
)

staging_songs_copy = ("""
copy staging_songs 
from {}
iam_role {}
json 'auto'
region {}
""").format(
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['CLUSTER']['REGION']
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP  'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time,
       e.user_id,
       e.level,
       s.song_id,
       s.artist_id,
       e.session_id,
       e.location,
       e.user_agent  
FROM staging_events e
JOIN staging_songs s 
    ON (
        e.song = s.title
        AND
        e.length = s.duration
        AND
        e.artist = s.artist_name
        )
WHERE e.page='NextSong'
;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id,
       first_name,
       last_name,
       gender,
       level
FROM staging_events
;
""")

song_table_insert = ("""
INSERT INTO song (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
       title,
       artist_id,
       year,
       duration
FROM staging_songs
;
""")

artist_table_insert = ("""
INSERT INTO artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
       artist_name,
       artist_location,
       artist_latitude,
       artist_longitude
FROM staging_songs
;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' start_time,
    EXTRACT(hour from start_time),
    EXTRACT(day from start_time),
    EXTRACT(week from start_time),
    EXTRACT(month from start_time),
    EXTRACT(year from start_time),
    EXTRACT(weekday from start_time)
FROM staging_events 
;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
