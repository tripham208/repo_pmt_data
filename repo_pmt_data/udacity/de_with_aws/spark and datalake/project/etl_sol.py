import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data_path = os.path.join(input_data, 'song_data', '*', '*', '*')

    # read song data file
    df = spark.read.json(song_data_path)

    # extract columns to create songs table
    songs_table = df.select(
        F.col("song_id"),
        F.col("title"),
        F.col("artist_id"),
        F.col("year"),
        F.col("duration")
    )

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs'), partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select(
        df["artist_name"].alias("name"),
        df["artist_location"].alias("location"),
        df["artist_latitude"].alias("latitude"),
        df["artist_longitude"].alias("longitude")
    ).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data_path = os.path.join(input_data, 'log_data', '*', '*')

    # read log data file
    df = spark.read.json(log_data_path)

    # filter by actions for song plays
    df = df.filter(df["page"] == "NextSong")

    # extract columns for users table
    users_table = df.select(
        F.col("userId").alias("user_id"),
        F.col("firstName").alias("first_name"),
        F.col("lastName").alias("last_name"),
        F.col("gender"),
        F.col("level")
    ).dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn('start_time', get_timestamp(df.ts))

    # extract columns to create time table
    time_table = df.select(
        F.hour("start_time").alias("hour"),
        F.dayofmonth("start_time").alias("day"),
        F.weekofyear("start_time").alias("week"),
        F.month("start_time").alias("month"),
        F.year("start_time").alias("year"),
        F.dayofweek("start_time").alias("weekday"),
    ).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time'), partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data', '*', '*', '*'))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(
        song_df,
        song_df.artist_name == df.artist,
        'inner'
    ).withColumn("songplay_id", F.monotonically_increasing_id()).select(
        F.col("songplay_id"),
        F.col("start_time"),
        F.col("userId").alias("user_id"),
        F.col("level"),
        F.col("song_id"),
        F.col("artist_id"),
        F.col("sessionId").alias("session_id"),
        F.col("location"),
        F.col("userAgent").alias("user_agent"),
        F.month("start_time").alias("month"),
        F.year("start_time").alias("year"),
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), partitionBy=['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://pmt-bucket/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
