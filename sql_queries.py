import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS log_data"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_data"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS log_data (
    artist             VARCHAR(255),
    auth               VARCHAR(50),
    first_name         VARCHAR(255),
    gender             VARCHAR(1),
    item_in_session    INTEGER,
    last_name          VARCHAR(255), 
    length             DOUBLE PRECISION,
    level              VARCHAR(50),
    location           VARCHAR(255), 
    method             VARCHAR(25),
    page               VARCHAR(35),
    registration       BIGINT,
    session_id         INTEGER,
    song               VARCHAR(255),
    status             INTEGER,
    ts                 VARCHAR(50) NOT NULL,
    user_agent         VARCHAR(255),
    user_id            INTEGER)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS song_data (
    num_songs         INTEGER NOT NULL,
    artist_id         VARCHAR(255) NOT NULL,
    artist_latitude   DOUBLE PRECISION,
    artist_longitude  DOUBLE PRECISION,
    artist_location   VARCHAR(255),
    artist_name       VARCHAR(255) NOT NULL,
    song_id           VARCHAR(255) NOT NULL,
    title             VARCHAR(255) NOT NULL,
    duration          DOUBLE PRECISION NOT NULL,
    year              INTEGER NOT NULL,
    PRIMARY KEY (song_id))
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay (
    songplay_id        INT IDENTITY (0,1) sortkey,
    start_time         TIMESTAMP NOT NULL,
    user_id            INTEGER NOT NULL,
    level              VARCHAR(50),
    song_id            VARCHAR(255) NOT NULL,
    artist_id          VARCHAR(255) NOT NULL,
    session_id         INTEGER NOT NULL,
    location           VARCHAR(255),
    user_agent         VARCHAR(255),
    PRIMARY KEY (songplay_id))
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users (
    user_key    INT IDENTITY (0,1) sortkey,
    user_id     INTEGER,
    first_name  VARCHAR(255),
    last_name   VARCHAR(255),
    gender      VARCHAR(1),
    level       VARCHAR(50),
    PRIMARY KEY (user_key, user_id))
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs (
    song_key     INT IDENTITY (0,1) sortkey,
    song_id      VARCHAR(255) NOT NULL,
    title        VARCHAR(255),
    artist_id    VARCHAR(255),
    year         INTEGER,
    duration     DOUBLE PRECISION,
    PRIMARY KEY (song_key, song_id))
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists (
    artist_key  INT IDENTITY (0,1) sortkey,
    artist_id   VARCHAR(255) NOT NULL,
    name        VARCHAR(255),
    location    VARCHAR(255),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    PRIMARY KEY (artist_key, artist_id))
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
    time_key     INT IDENTITY (0,1) sortkey,
    start_time   TIMESTAMP NOT NULL,
    hour         INTEGER,
    day          INTEGER,
    week         INTEGER,
    month        INTEGER,
    year         INTEGER,
    weekday      VARCHAR(10),
    PRIMARY KEY (time_key))
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY log_data FROM {} 
    CREDENTIALS 'aws_iam_role={}' 
    COMPUPDATE OFF REGION 'us-west-2'
    FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY song_data FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF REGION 'us-west-2'
    JSON 'auto'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)        
SELECT 
    timestamp 'epoch' + cast(l.ts as bigint) * interval '0.001 second' as start_time,
    l.user_id,
    l.level,
    s.song_id,
    s.artist_id,
    l.session_id,
    l.location,
    l.user_agent
FROM log_data l
JOIN song_data s ON (l.artist=s.artist_name AND l.song=s.title AND l.length=s.duration)
WHERE l.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO dim_users (user_id, first_name, last_name, gender, level)                
SELECT user_id, first_name, last_name, gender, level
FROM log_data
""")

song_table_insert = ("""
INSERT INTO dim_songs (song_id, title, artist_id, year, duration)                
SELECT song_id, title, artist_id, year, duration
FROM song_data
""")

artist_table_insert = ("""
INSERT INTO dim_artists (artist_id, name, location, latitude, longitude)                
SELECT 
artist_id, 
artist_name as name, 
artist_location as location, 
artist_latitude as latitude, 
artist_longitude as longitude
FROM song_data
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)                
SELECT timestamp 'epoch' + cast(ts as bigint) * interval '0.001 second' as start_time,
EXTRACT (hour FROM start_time) as hour,
EXTRACT (day FROM start_time) as day,
EXTRACT (week FROM start_time) as week,
EXTRACT (month FROM start_time) as month,
EXTRACT (year FROM start_time) as year,
EXTRACT (weekday FROM start_time) as weekday
FROM log_data
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
