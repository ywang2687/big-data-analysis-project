from kafka import KafkaConsumer, TopicPartition
from report_pb2 import Report
import threading

broker = 'localhost:9092'
consumer = KafkaConsumer(bootstrap_servers=[broker], 
                        group_id = "debug",
                        auto_offset_reset = "latest")
consumer.subscribe("temperatures")
#print(consumer.assignment())

while True:
    batch = consumer.poll(1000)
    for tp, messages in batch.items():
        for msg in messages:
    #        print(msg)
            report = Report.FromString(msg.value)
            print( {'partition': msg.partition,
                        'key': msg.key.decode('utf-8') if msg.key else None,
                        'date': report.date,
                        'degrees': report.degrees})

