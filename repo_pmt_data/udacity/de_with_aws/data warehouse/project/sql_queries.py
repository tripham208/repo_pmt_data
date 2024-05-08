import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config["IAM_ROLE"]["ARN"]

LOG_DATA = config["S3_PMT"]["LOG_DATA"]
SONG_DATA = config["S3_PMT"]["SONG_DATA"]
LOG_JSONPATH = config["S3_PMT"]["LOG_JSONPATH"]

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
        region 'us-east-2'
        format as json {opt}
        """
# gzip delimiter ';' compupdate off region 'us-west-2'

"""
        copy staging_songs from 's3://pmt-uda-bucket-us-east-2/song_data/A' 
        iam_role 'arn:aws:iam::016036110502:role/pmt_redshift' region 'us-east-2' format as json 'auto';
        
                copy staging_events from 's3://pmt-uda-bucket-us-east-2/song_data/2018-11-12-events.json' 
        iam_role 'arn:aws:iam::016036110502:role/pmt_redshift'
        region 'us-east-2' format as json 's3://pmt-uda-bucket-us-east-2/song_data/log_json_path.json'

"""

# TABLE NAME
STAGING_EVENTS = "staging_events"
STAGING_SONGS = "staging_songs"

SONG_PLAY = "songplays"
USERS = "users"
SONGS = "songs"
ARTISTS = "artists"
TIME = "time"

# QUERY LISTS
drop_table_queries = [
    drop_table.format(table_name=STAGING_EVENTS),
    drop_table.format(table_name=STAGING_SONGS),
    drop_table.format(table_name=SONG_PLAY),
    drop_table.format(table_name=USERS),
    drop_table.format(table_name=SONGS),
    drop_table.format(table_name=ARTISTS),
    drop_table.format(table_name=TIME),

]

create_table_queries = [
    create_table.format(
        table_name=STAGING_SONGS,
        atbs=f"""
            artist_id           VARCHAR(100),
            artist_latitude     VARCHAR(100),
            artist_location     VARCHAR(100),
            artist_longitude    VARCHAR(100),
            artist_name         VARCHAR(100),
            duration            FLOAT,
            num_songs           INT,
            song_id             VARCHAR(100),
            title               VARCHAR(100),
            year                INT
        """
    ), create_table.format(
        table_name=STAGING_EVENTS,
        atbs=f"""
            artist              VARCHAR(100),
            auth                VARCHAR(100),
            firstName           VARCHAR(100),
            gender              VARCHAR(1),
            itemInSession       INT,
            lastName            VARCHAR(100),
            length              FLOAT,
            level               VARCHAR(20),
            location            VARCHAR(100),
            method              VARCHAR(10),
            page                VARCHAR(100),
            registration        BIGINT,
            sessionId           INT,
            song                VARCHAR(100),
            status              INT,
            ts                  BIGINT,
            userAgent           VARCHAR(1000),
            userId              BIGINT
        """
    ),
    create_table.format(
        table_name=USERS,
        atbs=f"""
            user_id             BIGINT       NOT NULL,
            first_name          VARCHAR(100) NOT NULL,
            last_name           VARCHAR(100) NOT NULL,
            gender              VARCHAR(1)   NOT NULL,
            level               VARCHAR(20)  NOT NULL,
            {create_primary_key.format(primary_keys="user_id")} 
        """
    ),
    create_table.format(
        table_name=SONGS,
        atbs=f"""
            song_id             VARCHAR(100) NOT NULL,
            artist_id           VARCHAR(100) NOT NULL,
            title               VARCHAR(100) NOT NULL, 
            year                INT          NOT NULL,
            duration            FLOAT        NOT NULL,
            {create_primary_key.format(primary_keys="song_id")} 
        """
    ),
    create_table.format(
        table_name=ARTISTS,
        atbs=f"""
            artist_id           VARCHAR(100) NOT NULL,
            name                VARCHAR(100) NOT NULL,
            location            VARCHAR(100),
            latitude            VARCHAR(100),
            longitude           VARCHAR(100),
            {create_primary_key.format(primary_keys="artist_id")} 
        """
    ),
    create_table.format(
        table_name=TIME,
        atbs=f"""
            start_time          BIGINT      NOT NULL,
            hour                SMALLINT    NOT NULL,
            day                 SMALLINT    NOT NULL,
            week                SMALLINT    NOT NULL,
            month               SMALLINT    NOT NULL,
            year                SMALLINT    NOT NULL,
            weekday             BOOLEAN,
            {create_primary_key.format(primary_keys="start_time")} 
        """
    ),
    create_table.format(
        table_name=SONG_PLAY,
        atbs=f"""
            songplay_id         BIGINT      NOT NULL IDENTITY(0,1),
            user_id             BIGINT      NOT NULL,
            song_id             VARCHAR(100)NOT NULL,
            artist_id           VARCHAR(100)NOT NULL,
            start_time          BIGINT      NOT NULL,
            level               VARCHAR(20) NOT NULL,
            session_id          BIGINT,
            location            VARCHAR(100),
            user_agent          VARCHAR(1000),
            {create_primary_key.format(primary_keys="songplay_id")},
            {create_foreign_key.format(foreign_key="user_id", table_references=USERS, col_references="user_id")},
            {create_foreign_key.format(foreign_key="song_id", table_references=SONGS, col_references="song_id")},
            {create_foreign_key.format(foreign_key="artist_id", table_references=ARTISTS, col_references="artist_id")},
            {create_foreign_key.format(foreign_key="start_time", table_references=TIME, col_references="start_time")}   
        """
    ),
]
# STAGING TABLES

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
        lever,
        ss.song_id,
        ss.artist_id,
        sessionId       AS session_id,
        location,
        userAgent       AS user_agent
    """,
    org_tbl=f""" 
        {STAGING_EVENTS} as se
        JOIN {STAGING_SONGS} as ss on (se.song = ss.title)
        where se.song is not null 
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
        ts as start_time, 
        extract(hour from  timestamp 'epoch' + ts/1000 * interval '1 second') as hour,
        extract(day from  timestamp 'epoch' + ts/1000 * interval '1 second') as day,
        extract(week from  timestamp 'epoch' + ts/1000 * interval '1 second') as week,
        extract(month from  timestamp 'epoch' + ts/1000 * interval '1 second') as month,
        extract(year from  timestamp 'epoch' + ts/1000 * interval '1 second') as year,
        extract(weekday from  timestamp 'epoch' + ts/1000 * interval '1 second') as weekday
    """,
    org_tbl=STAGING_EVENTS,
)

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]
insert_table_queries = [
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    songplay_table_insert
]
