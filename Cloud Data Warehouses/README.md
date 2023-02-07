# Summary:
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

# Data Architecture:
![Data Architecture](https://video.udacity-data.com/topher/2022/May/62770f73_sparkify-s3-to-redshift-etl/sparkify-s3-to-redshift-etl.png)

# Pre-preparation:
1. Create AWS Redshift cluster endpoint with your own AWS account
2. Create ARN to access to S3(ReadOnly permission) for AWS Redshift

# How to run the code
1. Go to "dwh.cfg" configration file, fill in your pre-defined endpoint of AWS Redshift cluster and ARN.
2. Go to terminal
3. Run "create_tables.py" to drop tables if exists and create tables
4. Run "etl.py" to copy the source data from AWS S3 bucket to staging database on AWS Redshift DataWarehouse and insert into proper target tables on AWS Redshift DataWarehouse.
5. Run "verify_tables.py" to verify the counts of table

# The design concept of DataWarehouse
#### Overview:
1. songplays - is designed to analyze the activity of users, so using "key distribution" strategy with user_id and "sorting key" strategy with start_time for improving the performance of query once order by start_time.
2. users,songs,time - flowing the same strategy.
3. artists - using "all distribution" strategy due to small table and speed up query.

## Fact Table
1. songplays - records in event data associated with song plays i.e. records with page NextSong
    * songplay_id(PRIMARY KEY), start_time(SORTKEY), user_id(DISTKEY), level, song_id, artist_id, session_id, location, user_agent


## Dimension Tables
2. users - users information from staging_events
    * user_id(PRIMARY KEY,DISTKEY,SORTKEY), first_name, last_name, gender, level
3. songs - songs information from staging_songs
    * song_id(PRIMARY KEY,DISTKEY,SORTKEY), title, artist_id, year, duration
4. artists - artists information from staging_songs
    * artist_id(PRIMARY KEY,SORTKEY), name, location, lattitude, longitude
5. time - timestamp information from staging_events
    * start_time(PARMARY KEY,DISTKEY,SORTKEY), hour, day, week, month, year, weekday


# Delete AWS Redshift cluster
Clean up AWS Redshift cluster once complete the project to save out billing.