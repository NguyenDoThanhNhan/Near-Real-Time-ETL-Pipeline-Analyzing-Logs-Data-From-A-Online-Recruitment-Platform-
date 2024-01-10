import os
import time
import datetime
import pyspark.sql.functions as sf
from uuid import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import when
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark import SparkConf, SparkContext
from uuid import * 
from uuid import UUID
import time_uuid 
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.window import Window as W
from pyspark.sql.functions import to_date, hour


spark = SparkSession.builder \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
    .config("spark.jars", "/usr/local/lib/mysql-connector-java-8.0.30.jar") \
    .getOrCreate()

def process_clicks(df):
    print('------------------------')
    print('Calculating clicks')
    print('------------------------')
    click_data = df.filter(df.custom_track=='click')
    click_data = click_data.na.fill({'bid':0})
    click_data = click_data.na.fill({'job_id':0})
    click_data = click_data.na.fill({'publisher_id':0})
    click_data = click_data.na.fill({'group_id':0})
    click_data = click_data.na.fill({'campaign_id':0})  
    click_data.createOrReplaceTempView('clickdata')
    click = spark.sql("""select date(ts) as date , hour(ts) as hour, job_id,publisher_id,campaign_id,group_id,
                round(avg(bid),2) as bid_set, sum(bid) as spend_hour, count(*) as clicks from clickdata
                group by date,hour,job_id,publisher_id,campaign_id,group_id""")
    click.show(5)
    print('------------------------')
    print('Finish clicks process')
    print('------------------------')
    return click

def process_conversion(df):
    print('------------------------')
    print('Calculating conversion')
    print('------------------------')
    conversion_data = df.filter(df.custom_track=='conversion')
    conversion_data = conversion_data.na.fill({'job_id':0})
    conversion_data = conversion_data.na.fill({'publisher_id':0})
    conversion_data = conversion_data.na.fill({'group_id':0})
    conversion_data = conversion_data.na.fill({'campaign_id':0})
    conversion_data.createOrReplaceTempView('conversiondata')
    conversion = spark.sql("""select date(ts) as date , hour(ts) as hour, job_id,publisher_id,campaign_id,group_id,
                count(*) as conversion from conversiondata
                group by date,hour,job_id,publisher_id,campaign_id,group_id""")
    conversion.show(5)
    print('------------------------')
    print('Finish conversion process')
    print('------------------------')
    return conversion 

def process_qualified(df):
    print('------------------------')
    print('Calculating qualified')
    print('------------------------')
    qualified_data = df.filter(df.custom_track=='qualified')
    qualified_data = qualified_data.na.fill({'job_id':0})
    qualified_data = qualified_data.na.fill({'publisher_id':0})
    qualified_data = qualified_data.na.fill({'group_id':0})
    qualified_data = qualified_data.na.fill({'campaign_id':0})
    qualified_data.createOrReplaceTempView('qualifieddata')
    qualified = spark.sql("""select date(ts) as date , hour(ts) as hour, job_id,publisher_id,campaign_id,group_id,
                count(*) as qualified from qualifieddata
                group by date,hour,job_id,publisher_id,campaign_id,group_id""")
    qualified.show(5)
    print('------------------------')
    print('Finish qualified process')
    print('------------------------')
    return qualified

def process_unqualified(df):
    print('------------------------')
    print('Calculating unqualified')
    print('------------------------')
    unqualified_data = df.filter(df.custom_track=='unqualified')
    unqualified_data = unqualified_data.na.fill({'job_id':0})
    unqualified_data = unqualified_data.na.fill({'publisher_id':0})
    unqualified_data = unqualified_data.na.fill({'group_id':0})
    unqualified_data = unqualified_data.na.fill({'campaign_id':0})
    unqualified_data.createOrReplaceTempView('unqualifieddata')
    unqualified = spark.sql("""select date(ts) as date , hour(ts) as hour, job_id,publisher_id,campaign_id,group_id,
                count(*) as unqualified from unqualifieddata
                group by date,hour,job_id,publisher_id,campaign_id,group_id""")
    unqualified.show(5)
    print('------------------------')
    print('Finish unqualified process')
    print('------------------------')
    return unqualified

def process_final_data(click,conversion,qualified,unqualified):
    print('------------------------')
    print('Processing final data')
    print('------------------------')
    keys = ['job_id','date','hour','publisher_id','campaign_id','group_id']
    final_data = click.join(conversion,on=keys,how='full')\
        .join(qualified,on=keys,how='full')\
        .join(unqualified,on=keys,how='full')
    final_data = final_data.withColumnRenamed('date','dates')\
                .withColumnRenamed('hour','hours')\
                .withColumnRenamed('qualified','qualified_application')\
                .withColumnRenamed('unqualified','disqualified_application')
    final_data = final_data.withColumn('sources',lit('Cassandra'))
    final_data.show(10)
    print('------------------------')
    print('Finish processing final data')
    print('------------------------')
    return final_data

def extract_company_info(url,driver,user,password):
    print('------------------------')
    print('Extracting company_id from job dimension')
    print('------------------------')
    job = """(SELECT id as job_id, company_id, group_id, campaign_id FROM job) test"""
    company_data = spark.read.format("jdbc")\
            .option("driver","com.mysql.cj.jdbc.Driver")\
            .option("url","jdbc:mysql://localhost:3306/data_warehouse")\
            .option("dbtable","job")\
            .option("user","root")\
            .option("password","")\
            .load()
    company = company_data.select('id','company_id','group_id','campaign_id')
    company = company.withColumnRenamed('id','job_id')
    company.show(10)    
    print('------------------------')
    print('Finish extracing process')
    print('------------------------')
    return company

def import_data_to_mysql(df):
    df.write.format('jdbc')\
    .option('url','jdbc:mysql://localhost:3306/data_warehouse')\
    .option('driver','com.mysql.cj.jdbc.Driver')\
    .option('dbtable','events')\
    .option('user','root')\
    .option('password','')\
    .mode('append').save()
    return print('Importing data successfully')

def get_mysql_latest_time():    
    sql = """(select max(latest_update_time) from events) data"""
    mysql_time = spark.read.format("jdbc")\
            .option("driver","com.mysql.cj.jdbc.Driver")\
            .option("url","jdbc:mysql://localhost:3306/data_warehouse")\
            .option("dbtable",sql)\
            .option("user","root")\
            .option("password","")\
            .load()
    mysql_time = mysql_time.take(1)[0][0]
    if mysql_time is None:
        mysql_latest = '1998-01-01 23:59:59'
    else :
        mysql_latest = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_latest 

def maintask(mysql_latest):
    host = 'localhost'
    port = '3306'
    db_name = 'data_warehouse'
    user = 'root'
    password = ''
    url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
    driver = "com.mysql.cj.jdbc.Driver"
    print('------------------------')
    print('Read data from cassandra')
    print('------------------------')   
    tracking = spark.read.format("org.apache.spark.sql.cassandra").options(table='tracking',keyspace='datalake').load().where(col('ts')>= mysql_latest)
    print('------------------------')
    print('Read data from cassandra successfully')
    print('------------------------')
    tracking = tracking.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    tracking = tracking.filter(tracking.job_id.isNotNull()) 
#   data = process_data(tracking)
    click = process_clicks(tracking)
    conversion = process_conversion(tracking)
    qualified = process_qualified(tracking)
    unqualified = process_unqualified(tracking)
    company = extract_company_info(url,driver,user,password)
    final_data = process_final_data(click,conversion,qualified,unqualified)
    events = final_data.join(company,'job_id','left').drop(company.group_id).drop(company.campaign_id)
    events = events.select('job_id','dates','hours','disqualified_application','qualified_application','conversion','company_id','group_id','campaign_id','publisher_id','bid_set','clicks','spend_hour','sources')
    events.show(5)
    print('------------------------')   
    print('Loading data to MySQL')
    print('------------------------')
    import_data_to_mysql(events)
    return print('ETL Task Finished')

host = 'localhost'
port = '3306'
db_name = 'data_warehouse'
user = 'root'
password = ''
url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
driver = "com.mysql.cj.jdbc.Driver"

mysql_time = get_mysql_latest_time()
maintask(mysql_time)
spark.stop()

