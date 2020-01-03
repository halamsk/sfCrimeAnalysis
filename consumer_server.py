import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest', 
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe(['org.sfo.crime.data'])
for message in consumer :
    print(message.value)
