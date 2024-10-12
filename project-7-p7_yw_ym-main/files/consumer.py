from kafka import KafkaConsumer
from report_pb2 import Report
from kafka import TopicPartition
import sys
import json
import os
from datetime import datetime
import re


broker = "localhost:9092"
partitions = [int(p) for p in sys.argv[1:]]
consumer = KafkaConsumer(bootstrap_servers=[broker])
consumer = KafkaConsumer(bootstrap_servers=[broker])
consumer.assign([TopicPartition("temperatures", i) for i in partitions])

def load_partition(partition):
    filename = f"files/partition-{partition}.json"
    if not os.path.exists(filename):
        data = {"partition": partition, "offset": 0}
        with open(filename, 'w') as file:
            json.dump(data, file)
    else:
        with open(filename, 'r') as file:
            data = json.load(file)
    return data

partition_data = {partition: load_partition(partition) for partition in partitions}
#print(partition_data)

for p in partitions:
    consumer.seek(TopicPartition("temperatures", p), partition_data[p]["offset"])


while True:
    batch = consumer.poll(1000)
    for tp, messages in batch.items():
        partition = tp.partition

        for msg in messages:
            offset = consumer.position(tp)
           # print(offset)
            partition_data[partition]['offset'] = offset
            report_val = Report.FromString(msg.value)
           # print(report_val)
            date =report_val.date
           # print(type(date))
            degree = report_val.degrees
            month =  msg.key.decode('utf-8') if msg.key else None
           # print(month, degree, date)
            year = re.search(r'\d{4}', date).group()
            #print(month, degree, date,year)
            if month not in  partition_data[partition]:
                partition_data[partition][month] = {}
            if year not in partition_data[partition][month]:
                partition_data[partition][month][year] = {
                    "count": 1, "sum": degree, "avg": degree/1, "start": date, "end": date
                }
           # print(partition_data)
            partition_data[partition][month][year]
           
            if  datetime.strptime(date, "%Y-%m-%d") <=datetime.strptime(partition_data[partition][month][year]["end"] , "%Y-%m-%d"):
                print(date,partition_data[partition][month][year]["end"] )               
                continue
            else:
                if  datetime.strptime(date, "%Y-%m-%d") < datetime.strptime(partition_data[partition][month][year]["start"] , "%Y-%m-%d"):
                    partition_data[partition][month][year]["start"] = date
                print( partition_data[partition][month][year]["count"])
                partition_data[partition][month][year]["end"] = date  
                partition_data[partition][month][year]["count"] += 1
                partition_data[partition][month][year]["sum"] += degree
                partition_data[partition][month][year]["avg"] = partition_data[partition][month][year]["sum"] / partition_data[partition][month][year]["count"]
                
            
        path = f"files/partition-{partition}.json"
        path2 = path + ".tmp"
        with open(path2, "w") as f:
            json.dump(partition_data[partition], f)
            os.rename(path2, path)
            
                
