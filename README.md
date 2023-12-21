# Near-Real-Time-ETL-Pipeline-Analyzing-Logs-Data-From-A-Online-Recruitment-Platform-

**Introduction**
This project builds a near real time etl pipeline to analyze user's interactions logs data on a online recruitment platform. The project's objective is calculating clicks, candidates' conversions, qualified candidates, unqualified candidates of jobs posting on a online recruitment website/platform.

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

![Data Logs]([https://drive.google.com/file/d/19Qn9Q8wYOxYfNQUFMCaqTwtFhFcizYTJ/view?usp=sharing](https://imgur.com/a/gg3Aa7B)https://imgur.com/a/gg3Aa7B) 

**Processing Data**
