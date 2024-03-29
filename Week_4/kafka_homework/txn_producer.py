from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pykafka import KafkaClient

import json
import time

import random
kafka_client = KafkaClient(hosts="localhost:9092")
topic = kafka_client.topics['homework']
producer=topic.get_sync_producer()

client = MongoClient("mongodb+srv://cetingokhan:cetingokhan@cluster0.e1cjhff.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["sample_analytics"]
collection=db["transactions"]
while True:
    for i in range(random.randint(5,10)):
        random_document=collection.find_one()
        transactions=random_document["transactions"]
        transactions_count = len(transactions)
        selected_count = random.randint(1, transactions_count)
        selected_transactions = random.sample(transactions, selected_count)
        selected_transactions_array=[]
        for transaction in selected_transactions:
            selected_transaction={
                "account_id": random_document["account_id"],
                "date": transaction["date"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "amount": transaction["amount"],
                "transaction_code": transaction["transaction_code"],
                "symbol": transaction["symbol"],
                "price": transaction["price"],
                "total": transaction["total"]
            }
            with topic.get_sync_producer() as producer:
                producer.produce(json.dumps(selected_transaction).encode('utf-8'))
    time.sleep(2) 
   
