from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sqlalchemy as db
from pymongo import MongoClient
from google.cloud import storage, bigquery
from vnstock import listing_companies, stock_historical_data
import pandas as pd
import json
import csv


default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 4, 22),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['nguyenthang187txnm@gmail.com']
}

PATH_KEY = 'vnstock-381809-22dc568a0a39.json'
BUCKET_NAME = 'vnstock-data'
PROJECT = 'vnstock-381809'
DATASET_NAME = 'vnstock'

vnstock_data = [
    bigquery.SchemaField('Open', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('High', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Low', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Close', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Volume', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('TradingDate', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Ticker', 'STRING', mode='NULLABLE'),
]

vga_data = [
    bigquery.SchemaField('Index', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Item_id', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Title', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Brand', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Rating', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('Rating_num', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Price', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('Shipping', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('imgUrl', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Max_rslt', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Display_port', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('HDMI', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('DIRX', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Model', 'STRING', mode='NULLABLE'),
]

def vga_to_bq():
    client = bigquery.Client.from_service_account_json(PATH_KEY)

    storage_client = storage.Client.from_service_account_json(PATH_KEY)
    bucket = storage_client.get_bucket(BUCKET_NAME)

    filename = 'vga_data.csv'

    file_paths = f"gs://{BUCKET_NAME}/{filename}"
    table_name = filename.split('.')[0]

    job_config = bigquery.LoadJobConfig()
    job_config.schema = vga_data

    job_config.skip_leading_rows = 1
    job_config.max_bad_records=20
    job_config.source_format = bigquery.SourceFormat.CSV
    
    dataset_ref = client.dataset(DATASET_NAME, project=PROJECT)
    table_ref = dataset_ref.table(table_name)
    job = client.load_table_from_uri(
        file_paths, table_ref, job_config=job_config
    )
    job.result()

def vnstock_to_bq():
    client = bigquery.Client.from_service_account_json(PATH_KEY)

    storage_client = storage.Client.from_service_account_json(PATH_KEY)
    bucket = storage_client.get_bucket(BUCKET_NAME)

    filename = 'vnstock_data.csv'

    file_paths = f"gs://{BUCKET_NAME}/{filename}"
    table_name = filename.split('.')[0]
    job_config = bigquery.LoadJobConfig()
    job_config.schema = vnstock_data
    # job_config.max_bad_records=100
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV
    dataset_ref = client.dataset(DATASET_NAME, project=PROJECT)
    table_ref = dataset_ref.table(table_name)
    job = client.load_table_from_uri(
        file_paths, table_ref, job_config=job_config
    )
    job.result()

def tiki_to_bq():
    client = bigquery.Client.from_service_account_json(PATH_KEY)

    storage_client = storage.Client.from_service_account_json(PATH_KEY)
    bucket = storage_client.get_bucket(BUCKET_NAME)

    filename = 'tiki_data.json'
    
    file_paths = f"gs://{BUCKET_NAME}/{filename}"
    blob = bucket.blob(filename)
    table_name = filename.split('.')[0]
    table_ref = client.dataset(DATASET_NAME).table(table_name)
    job_config = bigquery.LoadJobConfig()
    job_config.ignore_unknown_values=True
    job_config.autodetect = True
    # job_config.max_bad_records=100
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_uri(
        file_paths,
        table_ref,
        job_config=job_config
    )
    job.result() 

def mysql_to_gcs():
    engine = db.create_engine("mysql+mysqlconnector://root:@localhost:3306/vga_info")
    connection = engine.connect()
    results = engine.execute("SELECT * FROM info")
    filename = 'vga_data.csv'
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(results)
    
    df = pd.read_csv(filename)
    df = df.fillna(0)
    df = df.dropna()
    df.to_csv(filename, index=False)
    
    storage_client = storage.Client.from_service_account_json(PATH_KEY)
    bucket = storage_client.get_bucket(BUCKET_NAME)
    bucket.blob(filename).upload_from_filename(filename)

def mongo_to_gcs():
    client = MongoClient('mongodb://localhost:27017/')
    mydb = client['tiki-product']
    mycol = mydb['product']

    filename = 'tiki_data.json'
    data = []
    for document in mycol.find():
        document['_id'] = str(document['_id'])
        data.append(document)

    with open(filename, "w", encoding='utf-8') as outfile:
        for doc in data:
            json.dump(doc, outfile, ensure_ascii=False)
            outfile.write('\n')

    storage_client = storage.Client.from_service_account_json(PATH_KEY)
    bucket = storage_client.get_bucket(BUCKET_NAME)
    # bucket.blob(filename).upload_from_string(str(data))
    bucket.blob(filename).upload_from_filename(filename)

def vnstock_to_gcs():
    filename = 'vnstock_data.csv'
    
    storage_client = storage.Client.from_service_account_json(PATH_KEY)
    bucket = storage_client.get_bucket(BUCKET_NAME)
    bucket.blob(filename).upload_from_filename(filename, timeout=1000)

with DAG('extract_transform_data', default_args=default_args, schedule_interval=None) as dag:
    mysql_to_gcs = PythonOperator(
        task_id='mysql_to_gcs',
        python_callable=mysql_to_gcs,
        dag=dag
    )
    mongo_to_gcs = PythonOperator(
        task_id='mongo_to_gcs',
        python_callable=mongo_to_gcs,
        dag=dag
    )
    vnstock_to_gcs = PythonOperator(
        task_id='vnstock_to_gcs',
        python_callable=vnstock_to_gcs,
        dag=dag
    )
    vga_to_bq = PythonOperator(
        task_id='vga_to_bq',
        python_callable=vga_to_bq,
        dag=dag
    )
    vnstock_to_bq = PythonOperator(
        task_id='vnstock_to_bq',
        python_callable=vnstock_to_bq,
        dag=dag
    )
    tiki_to_bq = PythonOperator(
        task_id='tiki_to_bq',
        python_callable=tiki_to_bq,
        dag=dag
    )

mysql_to_gcs >> vnstock_to_gcs >> mongo_to_gcs >> [vga_to_bq, vnstock_to_bq, tiki_to_bq]