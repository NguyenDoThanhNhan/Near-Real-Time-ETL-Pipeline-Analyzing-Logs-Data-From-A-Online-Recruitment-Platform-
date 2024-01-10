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

spark = SparkSession.builder.config("spark.jars", "/usr/local/lib/mysql-connector-java-8.0.30.jar").getOrCreate()

def get_latest_time_cassandra():
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'tracking',keyspace = 'datalake').load()
    cassandra_latest_time = data.agg({'ts':'max'}).take(1)[0][0]
    return cassandra_latest_time

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

def maintask():
    cassandra_latest_time = get_latest_time_cassandra()
    print('Cassandra latest time is {}'.format(cassandra_latest_time))
    mysql_latest = get_mysql_latest_time()
    print('MySQL latest time is {}'.format(mysql_latest))
    if cassandra_latest_time > mysql_latest : 
        print('Having new data to run the next task etl')
        spark.stop()
        return True
    else:
        spark.stop()
        return False

maintask()