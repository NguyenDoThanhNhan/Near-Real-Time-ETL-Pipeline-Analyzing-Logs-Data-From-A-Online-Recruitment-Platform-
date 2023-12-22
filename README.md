# Near-Real-Time-ETL-Pipeline-Analyzing-Logs-Data-From-A-Online-Recruitment-Platform-

**Introduction**
This project builds a near real-time etl pipeline to analyze users' interaction logs data on an online recruitment platform. The project's objective is to calculate clicks, candidates' conversions, qualified candidates, and unqualified candidates of jobs posting on an online recruitment website/platform.

**Technology Stack**
- Pyspack
- Kafka
- Airflow
- Docker
- Grafana
- Docker
- Cassandra
- MySQL
- Python

**Architechture**

**Logs Data**
- Logs data is stored in Cassandra
- Logs data schema:

root
 |-- create_time: string (nullable = false)
 
 |-- bid: double (nullable = true)
 
 |-- bn: string (nullable = true)
 
 |-- campaign_id: double (nullable = true)
 
 |-- cd: double (nullable = true)
 
 |-- custom_track: string (nullable = true)
 
 |-- de: string (nullable = true)
 
 |-- dl: string (nullable = true)
 
 |-- dt: string (nullable = true)
 
 |-- ed: string (nullable = true)
 
 |-- ev: double (nullable = true)
 
 |-- group_id: double (nullable = true)
 
 |-- id: string (nullable = true)
 
 |-- job_id: double (nullable = true)
 
 |-- md: string (nullable = true)
 
 |-- publisher_id: double (nullable = true)
 
 |-- rl: string (nullable = true)
 
 |-- sr: string (nullable = true)
 
 |-- ts: string (nullable = true)
 
 |-- tz: double (nullable = true)
 
 |-- ua: string (nullable = true)
 
 |-- uid: string (nullable = true)
 
 |-- utm_campaign: string (nullable = true)
 
 |-- utm_content: string (nullable = true)
 
...

 |-- utm_term: string (nullable = true)
 
 |-- v: double (nullable = true)
 
 |-- vp: string (nullable = true)

 ![logs_data](https://imgur.com/a/MeunFW0) 

**Processing Data**

- Screening and selecting essential information/columns such as: ["create_time"] , ["bid"], ["campaign_id"], ["custom_track"], ["group_id"], ["job_id"], ["publisher_id"], ["ts"]

- Filter ["job_id"] isNotNull and Replace Null values with 0.

- Scrutinize the logs data and Notice that values in the column ["custom_track"] are useful to analyze candidates, including clicks, conversion, unqualified, and qualified. 

- Filter these values in the column ["custom_track"] and Calculate basic values from data for in-depth analysis.

- Write Spark jobs and ETL process using pySpark.

- Store data in Data Warehouse - MySQL for further in-depth analysis after processing it.

- Use Airflow to monitor and automate spark job.

**Processed & Clean Data**

root

 |-- job_id: double (nullable = true)
 
 |-- dates: date (nullable = true)
 
 |-- hours: integer (nullable = true)
 
 |-- disqualified_application: long (nullable = true)
 
 |-- qualified_application: long (nullable = true)
 
 |-- conversion: long (nullable = true)
 
 |-- company_id: integer (nullable = true)
 
 |-- group_id: double (nullable = true)
 
 |-- campaign_id: double (nullable = true)
 
 |-- publisher_id: double (nullable = true)
 
 |-- bid_set: double (nullable = true)
 
 |-- clicks: long (nullable = true)
 
 |-- spend_hour: double (nullable = true)
 
 |-- sources: string (nullable = true)
 
 |-- latest_update_time: timestamp (nullable = true)


