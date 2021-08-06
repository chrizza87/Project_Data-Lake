# Udacity Data Engineer Course / Project 4: Data Lake
## 1 Description
The data from Sparkify resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 
The data needs to be extracted from S3, processed using Spark and loaded back into S3 as a set of dimensional tables with an ETL pipeline.

Fact tables:
- songplays

Dimension tables:
- users
- songs
- artists
- time

## 2 Project Setup
### 2.1 Prerequisites
The following tools/packages/frameworks have to be installed on your system
- python3 with pyspark

### 2.2 Run the project
1. insert your aws credentials to ```dl.cfg```
2. change the output_data variable (row 178) in ```etl.py``` to your needs (own s3 bucket for example)
2. run ```python3 etl.py``` to process data from s3 (song_data and log_data), extract the data and save all data in parquet files