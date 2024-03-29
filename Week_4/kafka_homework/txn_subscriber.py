import csv
import json
from pykafka import KafkaClient

kafkaClient = KafkaClient(hosts="localhost:9092")
topic = kafkaClient.topics['homework']

with open("collected_event.csv",mode='w',newline='')as file:
    writer=csv.writer(file)
    consumer=topic.get_simple_consumer()
    i=1
    for message in consumer:
        if message is not None:
            message_data=json.loads(message.value.decode('utf-8'))
            writer.writerow([
                message_data['account_id'],
                message_data['date'],
                message_data['amount'],
                message_data['transaction_code'],
                message_data['symbol'],
                message_data['price'],
                message_data['total']
            ])
            print(f"{i}.message inserted to CSV file.")
        i=i+1    

   