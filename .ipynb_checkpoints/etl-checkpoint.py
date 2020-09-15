import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Long, TimestampType as Timestamp
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Extract data from song_data and write songs and artists table
    
    Arguments:
    - spark : SparkSession object
    - input_data : input data root dir path
    - output_data : output data root dir path
    """
    # schema for song_data 
    songSchema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year",Int())
    ])
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data, schema=songSchema).dropDuplicates(["song_id"])

    # extract columns to create songs table
    df.createOrReplaceTempView("song_data")
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration FROM song_data
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs_table", 'overwrite')

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude 
        FROM song_data
    """).dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists_table")


def process_log_data(spark, input_data, output_data):
    """
    Extract data from log data and write users, time and songplays table
    
    Arguments:
    - spark : SparkSession object
    - input_data : input data root dir path
    - output_data : output data root dir path
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*"

    # read log data file
    df = spark.read.json(log_data)

    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table
    df.createOrReplaceTempView("log_data")
    users_table = spark.sql("""
        SELECT lg.userId as user_id, lg.firstName as first_name, lg.lastName as last_name, lg.gender, lg.level FROM log_data lg
        JOIN (
        SELECT userId, MAX(ts) as latest FROM log_data
        GROUP BY userId
        ) max_ts 
        ON lg.userId = max_ts.userId
        WHERE lg.ts = max_ts.latest
    """).dropDuplicates(["user_id"])
    
    # write users table to parquet files
    users_table.write_parquet(output_data + "users_table")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : (x // 1000), Long())
    df = df.withColumn("unix_timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : datetime.fromtimestamp(x), Timestamp())
    df = df.withColumn('datetime', get_datetime("unix_timestamp")) 
    
    # extract columns to create time table
    df.createOrReplaceTempView("log_data_time")
    time_table = spark.sql("""
        SELECT unix_timestamp as start_time,
        EXTRACT(hour from datetime) as hour,
        EXTRACT(day from datetime) as day,
        EXTRACT(week from datetime) as week,
        EXTRACT(month from datetime) as month,
        EXTRACT(year from datetime) as year,
        dayofweek(datetime) as weekday
        FROM log_data_time
    """).dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time_table")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*", schema=songSchema)
    
    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("song_data")
    time_table.createOrReplaceTempView("time_table")
    songplays_table = spark.sql("""
        SELECT unix_timestamp as start_time,
        t.year as year,
        t.month as month,
        userId,
        level,
        song_id,
        artist_id,
        sessionId,
        location,
        userAgent
        FROM log_data_time lg
        LEFT JOIN song_data s ON (lg.song = s.title and lg.artist = s.artist_name)
        LEFT JOIN time_table t ON (lg.unix_timestamp = t.start_time)
    """)

    songplays_table = songplays_table.withColumn('songplay_id', F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month

    songplays_table.write.partitionBy("year","month").parquet(output_data + "songplays_table")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://dend-dwh-hgl/Data/data_lake_output
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
