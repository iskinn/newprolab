from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


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
    'Gobblin_Kafka_to_HDFS',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1
)

t1 = BashOperator(
    task_id='Start_Gobblin_standalone',
    bash_command='/root/gobblin_kafka_to_hdfs.sh ',
    retries=0,
    dag=dag)

t2 = BashOperator(
    task_id='Stop_Gobblin_standalone',
    bash_command='sleep 30 && /opt/gobblin/gobblin-dist/bin/gobblin-standalone.sh stop ',
    retries=0,
    dag=dag)

#t2.set_upstream(t1)
#t3.set_upstream(t2)
