from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

import pandas as pd
import requests

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from datetime import datetime, timedelta
import psycopg2

# A DAG represents a workflow, a collection of tasks
with DAG(
    dag_id="homework",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    schedule_interval="*/5 * * * *") as dag:

    client = MongoClient("mongodb+srv://cetingokhan:cetingokhan@cluster0.e1cjhff.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")


    def generate_random_heat_and_humidity_data(dummy_record_count:int):
        import random
        import datetime
        from models.heat_and_humidity import HeatAndHumidityMeasureEvent
        records = []
        for i in range(dummy_record_count):
            temperature = random.randint(10, 40)
            humidity = random.randint(10, 100)
            timestamp = datetime.datetime.now()
            creator = "gokhan"
            record = HeatAndHumidityMeasureEvent(temperature, humidity, timestamp, creator)
            records.append(record)
        return records
    
    def save_data_to_mongodb(records):
        db = client["bigdata_training"]
        collection = db["sample_coll"]
        for record in records:
            collection.insert_one(record.__dict__)


    def create_sample_data_on_mongodb():
        records = generate_random_heat_and_humidity_data(10)
        save_data_to_mongodb(records)
        


    def copy_anomalies_into_new_collection():        
        # sample_coll collectionundan temperature 30 dan büyük olanları new(kendi adınıza bir collectionname) 
        # collectionuna kopyalayın(kendi creatorunuzu ekleyin)
        
        client = MongoClient("mongodb+srv://cetingokhan:cetingokhan@cluster0.e1cjhff.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
        collection = client ["bigdata_training"] ["sample_coll"]
        collection_2 =client ["bigdata_training"] ["gokhan_anomalies"]
        temp_query={"creator":"gokhan","temperature": {"$gt": 30}}
        anomalies = collection.find(temp_query)
        
        for anomaly in anomalies:
            anomaly["creator"]="gokhan"
            try:
                collection_2.insert_one(anomaly)
            except Exception as exp:
                print(f"hatali")
               

    def copy_airflow_logs_into_new_collection():            
        conn = psycopg2.connect(
            host="postgres",
            port="5432",
            database="airflow",
            user="airflow",
            password="airflow"
        )

        # airflow veritababnındaki log tablosunda bulunan verilerin son 1 dakikasında oluşan event bazındaki kayıt sayısını 
        # mongo veritabanında oluşturacağınız"log_adınız" collectionına event adı ve kayıt sayısı bilgisi ile 
        # birlikte(güncel tarih alanına ekleyerek) yeni bir tabloya kaydedin.
        # Örn çıktı;
        #{
        #    "event_name": "task_started",
        #    "record_count": 10,
        #    "created_at": "2022-01-01 00:00:00"
        #}
        cursor = conn.cursor()
        sql_query="""SELECT event,COUNT(*),NOW() FROM log WHERE execution_date> NOW()- INTERVAL '1 minute' GROUP BY event"""
        cursor.execute(sql_query)
        records=cursor.fetchall()
        cursor.close()

        client = MongoClient("mongodb+srv://cetingokhan:cetingokhan@cluster0.e1cjhff.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
        db = client["bigdata_training"]
        log=db["log_gokhan"]

        for record in records:
            event_name=records[0]
            record_count=record[1]
            created_at = record[2].strftime("%Y-%m-%d %H:%M:%S")

            document={
                "event_name":event_name,
                "record_count":record_count,
                "created_at":created_at
            }
            log.insert_one(document)
            
        





    dag_start = DummyOperator(task_id="start")
    dag_final = DummyOperator(task_id="final_task")
    create = PythonOperator(task_id="create_sample_data",python_callable=create_sample_data_on_mongodb)
    copy_anomalies = PythonOperator(task_id="copy_anomalies_into_new_collections",python_callable=copy_anomalies_into_new_collection)
    copy_airflow = PythonOperator(task_id="insert_airflow_logs_into_mongodb",python_callable=copy_airflow_logs_into_new_collection)


    dag_start >>[create,copy_airflow]
    create >> copy_anomalies >> dag_final
    copy_airflow >> dag_final
    