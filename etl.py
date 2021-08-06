import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates the spark session
    
    Returns the spark session object
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads the song_data json files, creates temp views and saves selected data columns 
    for songs and artists as parquet files

    Parameters
    ----------
    spark: spark session object
    input_data: input data base path
    output_data: output data base path
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT DISTINCT 
                            song_id, 
                            title, 
                            artist_id, 
                            year, 
                            duration
                            FROM songs
                            WHERE song_id IS NOT NULL 
                            """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs/")

    # extract columns to create artists table
    artists_table = spark.sql("""
                              SELECT DISTINCT 
                              artist_id, 
                              artist_name as name, 
                              artist_location as location, 
                              artist_latitude as lattitude, 
                              artist_longitude as longitude
                              FROM songs
                              WHERE artist_id IS NOT NULL
                              """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    """
    Reads the log_data json files, creates an temp view and saves selected data columns 
    for users, time and songplays as parquet files

    Parameters
    ----------
    spark: spark session object
    input_data: input data base path
    output_data: output data base path
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    
    # FOR LOCAL DEV WITHOUT S3
    # log_data = input_data + "log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df.filter(df.page=="NextSong").createOrReplaceTempView("logs")

    # extract columns for users table    
    users_table = spark.sql("""
                              SELECT DISTINCT 
                              userId as user_id, 
                              firstName as first_name, 
                              lastName as last_name, 
                              gender, 
                              level
                              FROM logs
                              WHERE userId IS NOT NULL
                            """)
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(x) / 1000.0))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    # first convert ms in s (/ 1000) and then use fromtimestamp function
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x) / 1000.0), TimestampType())
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    df.createOrReplaceTempView("time")
    time_table = spark.sql("""
                           SELECT DISTINCT
                           datetime as start_time, 
                           hour(datetime) as hour, 
                           day(datetime) as day, 
                           weekofyear(datetime) as week, 
                           month(datetime) as month, 
                           year(datetime) as year, 
                           dayofweek(datetime) as weekday
                           FROM time
                           WHERE datetime IS NOT NULL  
                           """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time/")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/")
    song_df.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT
                                monotonically_increasing_id() as songplay_id,
                                FROM_UNIXTIME(ts/1000) as start_time,
                                l.userId as user_id,
                                l.level,
                                s.song_id,
                                s.artist_id,
                                l.sessionId as session_id,
                                l.location,
                                l.userAgent as user_agent
                                FROM logs l
                                LEFT JOIN songs s ON (s.title = l.song)
                                """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").parquet(output_data + "songplays/")


def main():
    """
    Main method to set input and output data base paths and trigger process_song_data and process_log_data method
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/output"
    
    # FOR LOCAL DEV WITHOUT S3
    # input_data = "data/"
    # output_data = "output/"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
