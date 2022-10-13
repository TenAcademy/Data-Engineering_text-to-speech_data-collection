# importing the required libraries
import os
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import boto3
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# import defaults as defs

# ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
# connection_string = defs.conn_string

default_args = {
    # 'start_date': days_ago(5),
    'owner': 'f0x_tr0t',
    'depends_on_past': False,
    'email': ['fisseha.137@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'tags': ['week7', 'Kafka clusters']
}

# kafka node server
aws_instance_bootstrap_servers = ['b-1.batch6w7.6qsgnf.c19.kafka.us-east-1.amazonaws.com:9092',
                                  'b-2.batch6w7.6qsgnf.c19.kafka.us-east-1.amazonaws.com:9092']

# raw data topic
RAW_DATA_TOPIC = 'g1-raw-text-data-topic'

# define the DAG
etl_dag = DAG(
    'speech-collection.Kafka',
    default_args=default_args,
    start_date=datetime(2022, 10, 1),
    description='An end to end data pipeline for week 7 of 10 academy project',
    schedule=timedelta(days=1),     # run every day
    catchup=False                   # dont perform a backfill of missing runs
)


# region trigger task

def receive_trigger():
    print('full data pipeline trigger received')


trigger_pipeline = PythonOperator(
    task_id='trigger_pipeline',
    python_callable=receive_trigger,
    dag=etl_dag
)

# endregion


# region read raw data

def read_raw_data(ti):
    print('reading raw data . . .')

    print(f'now in {os.getcwd()}')
    raw_data = pd.read_csv('./data/raw_data.csv')
    print(f'shape of the data: {raw_data.shape}')
    raw_data_json = raw_data.iloc[0:3, ].to_json(orient="table",
                                                 index=False).encode('utf-8')
    raw_data_json_parsed = json.loads(raw_data_json)
    ti.xcom_push(key='raw_data_from_csv', value=raw_data_json_parsed)

    print(f'raw_data_json: {raw_data_json}\ntype: {type(raw_data_json)}')
    print(f'raw_data_json_parsed: {raw_data_json_parsed}\n' +
          f'type: {type(raw_data_json_parsed)}')
    print(json.dumps(raw_data_json_parsed, indent=4))

    print('reading raw data completed . . .')


raw_data_read = PythonOperator(
    task_id='raw_data_read',
    python_callable=read_raw_data,
    dag=etl_dag
)

# endregion


# region put raw data into a S3 bucket
def put_raw_data_to_S3(ti):
    print('putting raw data to S3 bucket . . .')

    raw_data = ti.xcom_pull(key='raw_data_from_csv', task_ids='raw_data_read')
    print(f'raw data: {raw_data}\ntype of the data: {type(raw_data)}')
    print('raw message received from previous task. . .')

    df_raw_data = pd.DataFrame(pd.Series(raw_data))
    print('raw message converted to a data frame . . .')

    real_S3bucket = "/mnt/10ac-batch-6/notebooks/g1-dlk"
    S3bucket = "./s3bucket/"
    data_folder = "./data/"
    file_name = datetime.now().strftime("%Y%m%d-%H%M%S") + ".csv"
    print('putting raw data to S3 bucket . . .')
    df_raw_data.to_csv(S3bucket + file_name, index=False)

    """s3 = boto3.client("s3")

    # format name with current timestamp
    json_file_name = datetime.now().strftime("%Y%m%d-%H%M%S") + ".json"
    s3object = s3.Object(S3bucket, json_file_name)

    s3object.put(Body=(bytes(json.dumps(raw_data).encode("UTF-8"))))"""

    print('putting raw data to S3 bucket completed. . .')


raw_data_to_S3 = PythonOperator(
    task_id='raw_data_to_S3',
    python_callable=put_raw_data_to_S3,
    dag=etl_dag
)

# endregion


# region produce message

def produce_the_message(ti):
    print('publishing raw message . . .')

    raw_data = ti.xcom_pull(key='raw_data_from_csv', task_ids='raw_data_read')
    print('raw message received from previous task. . .')
    print(f'raw data: {raw_data}\ntype of the data: {type(raw_data)}')

    df_raw_data = pd.DataFrame(pd.Series(raw_data))
    print('raw message converted to a data frame . . .')
    print(
        f'data frame raw data: {df_raw_data}\ntype of the data: {type(df_raw_data)}')
    # json_encoded_data = json.dumps(raw_data, indent=4, ensure_ascii=False)
    # print(f'json encoded data: {json_encoded_data}\ntype of the data: {type(json_encoded_data)}')

    print(f"extracted data: {df_raw_data.loc[['data']]}\nlength: {len(df_raw_data.loc[['data']])}\ntype: {type(df_raw_data.loc[['data']])}\ncols: {df_raw_data.loc[['data']].columns.values}\nshape: {df_raw_data.loc[['data']].shape}")
    print(f"df info: {df_raw_data.loc[['data']].info()}")
    print(
        f"series details: {df_raw_data.loc['data']}\ntype: {type(df_raw_data.loc['data'])}\nlen: {len(df_raw_data.loc['data'])}")
    print(
        f"series data details: {df_raw_data.loc['data'][0]}\ntype: {type(df_raw_data.loc['data'][0])}\nlen: {len(df_raw_data.loc['data'][0])}")
    for i in range(0, len(df_raw_data.loc['data'][0])):
        print(f"type: {type(df_raw_data.loc['data'][0][i])}")
        print(f"full data: {df_raw_data.loc['data'][0][i]}")
        print(f"headline: {df_raw_data.loc['data'][0][i]['headline']}")
        print(f"category: {df_raw_data.loc['data'][0][i]['category']}")
        print(f"date: {df_raw_data.loc['data'][0][i]['date']}")
        print(f"views: {df_raw_data.loc['data'][0][i]['views']}")
        print(f"article: {df_raw_data.loc['data'][0][i]['article']}")
        print(f"link: {df_raw_data.loc['data'][0][i]['link']}")

    print(f'publishing raw messages to the {RAW_DATA_TOPIC} topic . . .')
    producer = KafkaProducer(bootstrap_servers=aws_instance_bootstrap_servers)
    # producer = KafkaProducer(
    #   value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    # producer.send(RAW_DATA_TOPIC, {"text": raw_data['article'][0]})
    for i in range(0, len(df_raw_data.loc['data'][0])):
        byte_encoded_data = bytes(f"{df_raw_data.loc['data'][0][i]['article']}",
                                  encoding='utf-8')
        future = producer.send(RAW_DATA_TOPIC, byte_encoded_data)
        producer.flush()
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if produce request failed...
            log.exception()
            pass
        # Successful result returns assigned partition and offset
        print(f"topic: {record_metadata.topic}")
        print(f"value: {record_metadata}")
        print(f"partition: {record_metadata.partition}")
        print(f"offset: {record_metadata.offset}")
        print('publishing raw messages completed. . .')


produce_message = PythonOperator(
    task_id='produce_message',
    python_callable=produce_the_message,
    dag=etl_dag
)

# endregion


# region consume message

def consume_the_message():
    print('consuming raw messages . . .')
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(RAW_DATA_TOPIC,
                             # group_id='base-development-group',
                             bootstrap_servers=aws_instance_bootstrap_servers,
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             # StopIteration if no message after 1sec
                             consumer_timeout_ms=1000)

    for msg in consumer:
        print(f'in loop: {msg}')
        print(f"topic: {msg.topic}")
        print(f"partition: {msg.partition}")
        print(f"offset: {msg.offset}")
        print(f"value: {msg.value}")
        # decoded_data = json.dumps(msg, indent=4, ensure_ascii=False)
        decoded_data = msg.value.decode("utf-8")
        print(f"decoded data: {decoded_data}\ntype: {type(decoded_data)}")
    print('consuming raw messages completed . . .')


consume_message = PythonOperator(
    task_id='consume_message',
    python_callable=consume_the_message,
    dag=etl_dag
)

# endregion


# region transform and clean

def transform_and_prepare_the_message():
    print('message transformed and prepared')


transform_message = PythonOperator(
    task_id='transform_message',
    python_callable=transform_and_prepare_the_message,
    dag=etl_dag
)

# endregion


# region add meta data

def add_base_metadata():
    print('meta data added')


add_metadata = PythonOperator(
    task_id='add_metadata',
    python_callable=add_base_metadata,
    dag=etl_dag
)

# endregion


# region load to DWH

def load_to_DWH():
    print('data loaded to DWH')


load_message_to_DWH = PythonOperator(
    task_id='load_message_to_DWH',
    python_callable=load_to_DWH,
    dag=etl_dag
)

# endregion

trigger_pipeline >> raw_data_read >> raw_data_to_S3 >> produce_message >> consume_message >> transform_message >> add_metadata >> load_message_to_DWH
