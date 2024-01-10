from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
import multiprocessing
import logging 
import sys
root=logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

def consume_data_from_kafka(keyspace,kafka_topic):
    cluster = Cluster()
    session = cluster.connect(keyspace)
    consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers='192.168.64.1:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    consumer.poll(timeout_ms=2000)
    for message in consumer:
        data = message.value
        sql = """INSERT INTO tracking (create_time, bid, campaign_id, custom_track, group_id, job_id, publisher_id, ts) VALUES ('{}', {}, {}, '{}', {}, {}, {}, '{}')"""\
        .format(data['create_time'], data['bid'], data['campaign_id'], data['custom_track'], data['group_id'], data['job_id'], data['publisher_id'], data['ts'])
        print(sql)
        session.execute(sql)
        print("Read data from kafka topic and write to cassandra successfully")

        consumer.close()
        cluster.shutdown()
    
keyspace = "datalake"
kafka_topic = "tracking"
consume_data_from_kafka(keyspace,kafka_topic)

if __name__ == '__main__':
    process = multiprocessing.Process(target=consume_data_from_kafka,args=(keyspace,kafka_topic))
    process.start()
    process.join()