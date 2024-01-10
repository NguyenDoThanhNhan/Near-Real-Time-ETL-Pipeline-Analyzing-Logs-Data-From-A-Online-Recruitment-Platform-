from datetime import datetime, timedelta
from kafka import KafkaProducer
from cassandra.cqlengine.models import Model
import cassandra
import time
import uuid
import datetime
import math
import pandas as pd
pd.set_option("display.max_rows",None,"display.max_columns",None)
from sqlalchemy import create_engine
import numpy as np
import mysql.connector
import json
import random

host = 'localhost'
port = '3306'
db_name = 'data_warehouse'
user = 'root'
password = ''
url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
driver = "com.mysql.cj.jdbc.Driver"

kafka_producer = KafkaProducer(bootstrap_servers='192.168.64.1:9092',value_serializer=lambda x:json.dumps(x).encode('utf-8'))
kafka_topic = "tracking"

def get_data_from_job(user,password,host,database):
    cnx = mysql.connector.connect(user=user, password=password,
                                         host=host,
                                      database=database)
    query = """select id as job_id,campaign_id , group_id , company_id from job"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data

def get_data_from_publisher(user,password,host,database):
    cnx = mysql.connector.connect(user=user, password=password,
                                         host=host,
                                      database=database)
    query = """select distinct(id) as publisher_id from master_publisher"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data


def generating_dummy_data(n_records,user,password,host,db_name):
    publisher = get_data_from_publisher(user,password,host,db_name)
    publisher = publisher['publisher_id'].to_list()
    jobs_data = get_data_from_job(user,password,host,db_name)
    job_list = jobs_data['job_id'].to_list()
    campaign_list = jobs_data['campaign_id'].to_list()
    company_list = jobs_data['company_id'].to_list()
    group_list = jobs_data[jobs_data['group_id'].notnull()]['group_id'].apply(lambda x: int(x) if x.strip() else 'null').to_list()
    i = 0 
    fake_records = n_records
    while i <= fake_records:
        create_time = str(cassandra.util.uuid_from_time(datetime.datetime.now()))
        bid = random.randint(0,1)
        interact = ['click','conversion','qualified','unqualified']
        custom_track = random.choices(interact,weights=(70,10,10,10))[0]
        job_id = random.choice(job_list)
        publisher_id = random.choice(publisher)
        group_id = random.choice(group_list) 
        campaign_id = random.choice(campaign_list)
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data = {
            "create_time":create_time,
            "bid":bid,
            "campaign_id":campaign_id,
            "custom_track":custom_track,
            "group_id":group_id,
            "job_id":job_id,
            "publisher_id":publisher_id,
            "ts":ts,
        }
        print(data)
        kafka_producer.send(kafka_topic,value=data)
        i+=1 
    return print("Data Generated Successfully")

status = "ON"
while status == "ON":
    generating_dummy_data(n_records = random.randint(1,20),user = user , password = password , host = host , db_name = db_name)
    time.sleep(1)
kafka_producer.close()