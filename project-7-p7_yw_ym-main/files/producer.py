import time,threading
from kafka import KafkaAdminClient,KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
import weather
from report_pb2 import *



print(1)
broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])
print(2)

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(5) # Deletion sometimes takes a while to reflect

admin_client.create_topics([NewTopic(name="temperatures", num_partitions=4, replication_factor=1)])

print("Topics:", admin_client.list_topics())


producer = KafkaProducer(bootstrap_servers=[broker], retries =10, acks='all')


def get_month_name(date_str):
    month_names = ["January", "February", "March", "April", "May", "June",
                   "July", "August", "September", "October", "November", "December"]
    return month_names[int(date_str.split('-')[1]) - 1]

for date, degrees in weather.get_next_weather(delay_sec=0.1):
   # print(date)
    report = Report(date=date, degrees=degrees)
    report_bytes = report.SerializeToString()
    month_key = get_month_name(date).encode('utf-8')
    producer.send('temperatures', key=month_key, value=report_bytes)
    
