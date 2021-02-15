import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events(
                 artist          varchar,
                 auth            varchar,
                 firstName       varchar,
                 gender          varchar,
                 itemInSession   int,
                 lastName        varchar,
                 length          decimal,
                 level           varchar,
                 location        varchar,
                 method          varchar,
                 page            varchar,
                 registration    numeric,
                 sessionId       int,
                 song            varchar,
                 status          int,
                 ts              timestamp,
                 userAgent       varchar,
                 userId          varchar
                );""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                artist_id          varchar,
                artist_latitude    numeric,
                artist_longitude   numeric,
                artist_location    text,
                artist_name        varchar,
                song_id            varchar,
                title              varchar,
                year               int,        
                duration           numeric,        
                num_songs          int
                );""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                songplay_id int             IDENTITY(0,1),
                start_time  timestamp       REFERENCES   time(start_time) sortkey,
                user_id     varchar         REFERENCES   users(user_id)   distkey,
                level       varchar,
                song_id     varchar         REFERENCES   songs(song_id),
                artist_id   varchar         REFERENCES   artists(artist_id),
                session_id  int             NOT NULL,
                location    text,
                user_agent  varchar,
                UNIQUE (user_id, artist_id, start_time),
                PRIMARY KEY (songplay_id)
                );""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users(
                user_id     varchar         NOT NULL     sortkey,
                first_name  varchar,
                last_name   varchar,
                gender      text,
                level       text,
                PRIMARY KEY (user_id)
                );""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                song_id     varchar         NOT NULL     sortkey,
                title       varchar,
                artist_id   varchar         NOT NULL     distkey,
                year        int             NOT NULL,
                duration    numeric         NOT NULL,
                PRIMARY KEY (song_id)
                );""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                artist_id   varchar         NOT NULL     sortkey,
                name        varchar,
                location    text,
                latitude    numeric, 
                longitude   numeric,
                PRIMARY KEY (artist_id)
                );""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                start_time  timestamp      NOT NULL      sortkey,
                hour        smallint       NOT NULL,
                day         smallint       NOT NULL,
                week        smallint       NOT NULL,
                month       smallint       NOT NULL      distkey,
                year        smallint       NOT NULL,
                weekday     smallint       NOT NULL,
                PRIMARY KEY (start_time)
                );""")

# STAGING TABLES
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSON_PATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')
REGION = config.get('S3','REGION')

staging_events_copy = ("""
    copy staging_events from {} 
    credentials {}
    region {}
    format as json {}
    timeformat 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    ;""").format(LOG_DATA,IAM_ROLE,REGION,LOG_JSON_PATH)


staging_songs_copy = ("""
    copy songs from {} 
    credentials {}
    region {}
    format as json 'auto';""").format(SONG_DATA,IAM_ROLE,REGION)


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
        SELECT DISTINCT e.ts, 
                        e.userId, 
                        e.level, 
                        s.song_id, 
                        s.artist_id, 
                        e.sessionId, 
                        e.location, 
                        e.userAgent
        FROM staging_events e 
        INNER JOIN staging_songs s 
            ON e.song = s.title AND e.artist = s.artist_name
        WHERE e.page = 'NextSong';
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT e.userId, 
                        e.firstName, 
                        e.lastName, 
                        e.gender, 
                        e.level
        FROM staging_events e
        WHERE e.userId IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
        SELECT DISTINCT s.song_id, 
                        s.title, 
                        s.artist_id, 
                        s.year, 
                        s.duration
        FROM staging_songs s
        WHERE s.song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT s.artist_id, 
                        s.artist_name, 
                        s.artist_location,
                        s.artist_latitude,
                        s.artist_longitude
        FROM staging_songs s
        WHERE s.artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT  e.ts,
                        EXTRACT(hour from e.ts),
                        EXTRACT(day from e.ts),
                        EXTRACT(week from e.ts),
                        EXTRACT(month from e.ts),
                        EXTRACT(year from e.ts),
                        EXTRACT(weekday from e.ts)
        FROM staging_events e
        WHERE e.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [time_table_create, user_table_create, song_table_create, artist_table_create, staging_events_table_create, staging_songs_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
