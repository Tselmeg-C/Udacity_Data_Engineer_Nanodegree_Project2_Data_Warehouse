# Project: Data Warehouse

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs of user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project I have built an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## Project Datasets

The two datasets that reside in S3. Here are the S3 links for each:

***Song data:*** s3://udacity-dend/song_data  
***Log data:*** s3://udacity-dend/log_data   
***Log data json path:*** s3://udacity-dend/log_json_path.json   

Log in to your AWS consol and click [here](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend?region=us-west-2&tab=objects) to visit the datasets bucket on s3 .

### Song Dataset

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

song_data/A/B/C/TRABCEI128F424C983.json   
song_data/A/A/B/TRAABJL12903CDCF1A.json   

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

> {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### Log Dataset

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

log_data/2018/11/2018-11-12-events.json   
log_data/2018/11/2018-11-13-events.json   

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.
![song_data]()

## Schema for Song Play Analysis

A star schema optimized for queries on song play analysis was created. This includes the following tables.

### Fact Table

1. **songplays** - records in event data associated with song plays i.e. records with page 'NextSong'
- *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

### Dimension Tables

2. **users** - users in the app   
- *user_id, first_name, last_name, gender, level*  
3. **songs** - songs in music database    
- *song_id, title, artist_id, year, duration*    
4. **artists** - artists in music database    
- *artist_id, name, location, lattitude, longitude*    
5. **time** - timestamps of records in songplays broken down into specific units    
- *start_time, hour, day, week, month, year, weekday*   

## Project Template

The project template includes four files:

***create_table.py*** is where I created the fact and dimension tables for the star schema in Redshift.   
***etl.py***  is where I load data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.   
***sql_queries.py*** is where I define the SQL statements, that will be imported into the two other files above.   
***README.md*** is where I provide information on the project.   

## Project Steps

Below are steps I followed to complete each component of this project.

### Create Table Schemas

1. Design schemas for the fact and dimension tables   
2. Write a SQL CREATE statement for each of these tables in *sql_queries.py*    
3. Complete the logic in *create_tables.py* to connect to the database and create these tables
4. Write SQL DROP statements to drop tables in the beginning of *create_tables.py* if the tables already exist. This way, I can run *create_tables.py* whenever you want to reset my database and test my ETL pipeline.
5. Launch a redshift cluster and create an IAM role that has read access to S3.
6. Add redshift database and IAM role info to *dwh.cfg*.
7. Test by running *create_tables.py* and checking the table schemas in the redshift database. You can use Query Editor in the AWS Redshift console for this.

### Build ETL Pipeline

1. Implement the logic in *etl.py* to load data from S3 to staging tables on Redshift.
2. Implement the logic in *etl.py* to load data from staging tables to analytics tables on Redshift.
3. Test by running *etl.py* after running *create_tables.py* and running the analytic queries on the Redshift database to verify the result.
4. Delete the redshift cluster when finished.


