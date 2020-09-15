# Background
## Sparkify 
Sparkify services music streaming app. Sparkify team is interested in which songs their users listening to, with their streaming app. 

## Analytical Goals
- Goal of analysis is to understand what songs users are listening to
- Currently, usage log data and song data are stored in a JSON format, which is hard to query on. 
- Data engineers are to design and build Redshift DB using JSON data, so that Sparkify team can easily query their data. 

# Database Schema Design
Since this database is for analytical purpose, I chose star schema for analytical query performance. It is not normalized to 3NF form, so this schema may lacks in terms of handling redundancy and consistency. 

- Fact tables
    - song plays log table 
- Dimension tables
    - songs table
    - artist table
    - user table
    - time table

    
# ETL Pipeline
## ETL Pipeline Overview 
1. Songs and log JSON files (in S3 bucket) are loaded as Spark dataframe.
2. Using the Spark dataframes, we create output dataframes that correspond to fact & dimension tables.
3. Write output dataframes to S3 bucket as parquet file format

## Fact & Dimension Tables
- Extracting songs and artist data
    - Data for songs and artists relations are extracted from song_data (JSON format)
    - From song_data, 
        - song_id, title, artist_id, year, duration are loaded onto songs relation
        - artist_id, artist_name, artist_location, artist_latitude, artist_longitude are loaded onto artists relation
- Extracting user, time, log data
    - Data for user, time and song plays relations are extracted from log_data(JSON format)
    - From log_data,
        - ts (timestamp) data is loaded onto time relation. Hour, day, week of year, month and weekday values are extracted from ts before loaded. 
        - user_id, first_name, last_name, gender, level are loaded onto users relation
        - songplays table, which is a fact table, has ts, user_id, level, song_id, artist_id, session_id, location, user_agent as its columns 

# How to Run Pipeline

- In the EMR SSH, execute following command
~~~
spark-submit --master yarn etl.py
~~~

# Example Queries 
- Query below fetches the most played song in songplays table 
## Query 
```SQL
SELECT songplays.song_id, title, COUNT(*) FROM songplays
JOIN songs ON songplays.song_id = songs.song_id
GROUP BY songplays.song_id, title
ORDER BY COUNT(*) DESC
LIMIT 1
```
## Result 
| song_id            | title          | count |
| ------------------ | -------------- | ----- |
| SOBONKR12A58A7A7E0 | You're The One | 37    |