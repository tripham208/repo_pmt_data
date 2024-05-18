import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config["IAM_ROLE"]["ARN"]

LOG_DATA = config["S3"]["LOG_DATA"]
SONG_DATA = config["S3"]["SONG_DATA"]
LOG_JSONPATH = config["S3"]["LOG_JSONPATH"]

# SQL FORMAT
drop_table = "DROP TABLE IF EXISTS {table_name};"
create_table = "CREATE TABLE IF NOT EXISTS {table_name} ({atbs});"
create_primary_key = "PRIMARY KEY ({primary_keys})"
create_foreign_key = "FOREIGN KEY ({foreign_key}) references {table_references}({col_references})"
insert = "INSERT INTO {table_name} ({atbs}) VALUES ({values})"
insert_from_select = "INSERT INTO {des_tbl} ({atbs}) SELECT {values} FROM {org_tbl};"
select = "SELECT {atbs} FROM {table_name} {where} {order};"
create_index = "CREATE INDEX {index_name} ON {table_name} ({col})"
where = "WHERE {condition}"
order = "ORDER BY {order}"
s3_copy = """
        copy {table_name} from {s3_part} 
        iam_role {iam_role} 
        region 'us-west-2'
        format as json {opt}
        """

# TABLE NAME
STAGING_EVENTS = "staging_events"
STAGING_SONGS = "staging_songs"

SONG_PLAY = "songplays"
USERS = "users"
SONGS = "songs"
ARTISTS = "artists"
TIME = "time"

staging_events_copy = s3_copy.format(
    table_name=STAGING_EVENTS,
    s3_part=LOG_DATA,
    iam_role=DWH_ROLE_ARN,
    opt=f"""{LOG_JSONPATH}"""

)

staging_songs_copy = s3_copy.format(
    table_name=STAGING_SONGS,
    s3_part=SONG_DATA,
    iam_role=DWH_ROLE_ARN,
    opt="'auto'"
)

# FINAL TABLES

songplay_table_insert = insert_from_select.format(
    des_tbl=SONG_PLAY,
    atbs="start_time, user_id, level, song_id, artist_id, session_id, location, user_agent",
    values="""
    DISTINCT
        ts              AS start_time,
        userId          AS user_id,
        level,
        ss.song_id,
        ss.artist_id,
        sessionId       AS session_id,
        location,
        userAgent       AS user_agent
    """,
    org_tbl=f""" 
        {STAGING_EVENTS} as se
        JOIN {STAGING_SONGS} as ss on (se.song = ss.title)
        where se.song is not null and se.page = 'NextSong'
    """,
)

user_table_insert = insert_from_select.format(
    des_tbl=USERS,
    atbs="user_id, first_name, last_name, gender, level",
    values="""
    DISTINCT
        userid          AS user_id,
        firstName       AS first_name,
        lastName        AS last_name,
        gender,
        level  
    """,
    org_tbl=STAGING_EVENTS + " where userid is not null",
)

song_table_insert = insert_from_select.format(
    des_tbl=SONGS,
    atbs="song_id, title, artist_id, year, duration",
    values="""
    DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    """,
    org_tbl=STAGING_SONGS,
)

artist_table_insert = insert_from_select.format(
    des_tbl=ARTISTS,
    atbs="artist_id, name, location, latitude, longitude",
    values="""
    DISTINCT
        artist_id,
        artist_name         AS name,
        artist_location     AS "location",
        artist_latitude     AS latitude,
        artist_longitude    AS longitude
    """,
    org_tbl=STAGING_SONGS,
)

time_table_insert = insert_from_select.format(
    des_tbl=TIME,
    atbs="start_time, hour, day, week, month, year, weekday",
    values="""
    DISTINCT
        start_time, 
        extract(hour from  timestamp 'epoch' + start_time/1000 * interval '1 second') as hour,
        extract(day from  timestamp 'epoch' + start_time/1000 * interval '1 second') as day,
        extract(week from  timestamp 'epoch' + start_time/1000 * interval '1 second') as week,
        extract(month from  timestamp 'epoch' + start_time/1000 * interval '1 second') as month,
        extract(year from  timestamp 'epoch' + start_time/1000 * interval '1 second') as year,
        extract(weekday from  timestamp 'epoch' + start_time/1000 * interval '1 second') as weekday
    """,
    org_tbl=SONG_PLAY,
)
