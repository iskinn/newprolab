import elasticsearch
import csv, json
import subprocess
import pytz
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 14),
    'email': ['iskinn@inbox.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'concurrency': 1,
    'execution_timeout': timedelta(minutes=5)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'ElasticSearch_to_ClickHouse',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1
)

def extract_data_from_es(date_now, date_pass):

    # extract batch from ES
    print(" 111111 Elastic start 111111 ")
    print("date_now:",date_now, "date_pass:", date_pass)
    es = elasticsearch.Elasticsearch(["34.76.63.112:9200"])
    res = es.search(index="anton.guzenko", body={
        "query": {
            "range":
            {"@timestamp": {
                "gte": date_pass,
                "lt": date_now
            }
            }
        }
    }, size=5000)
    print( "22222 Write to file 22222")
    # dump batch to local csv file
    with open('/home/result.csv', 'w') as f:
        header_present = False
        for doc in res['hits']['hits']:
            msg = json.loads((doc['_source']['message']).replace(',,',',"",'))
            timestamp_int = int(round(msg["timestamp"]))
            msg["timestamp"] = timestamp_int
            msg["EventDate"] = datetime.utcfromtimestamp(timestamp_int).strftime('%Y-%m-%d')
            if not header_present:
                w = csv.DictWriter(f, msg.keys())
                w.writeheader()
                header_present = True

            w.writerow(msg)
    print( "33333 End write to file 33333")


def dag_task(ds, **kwargs):

    local_file_dir = '/home/'

    # read dagrun parameters
    date_now = kwargs['execution_date']
    date_pass = date_now - timedelta(minutes=15)

    # extract batch from elasticserach
    extract_data_from_es(date_now, date_pass)

t1 = PythonOperator(
    task_id='from_elasticsearch_to_csv', 
    python_callable=dag_task, 
    provide_context=True, 
    dag=dag,
)

t2 = BashOperator(
    task_id='from_csv_to_clickhouse',
    depends_on_past=False,
    bash_command='cat  /home/result.csv | clickhouse-client  --port 9001 --database=default --query="INSERT INTO anton_guzenko  (`head -n1 /home/result.csv`) FORMAT CSVWithNames"',
    dag=dag,
)

t1 >> t2
