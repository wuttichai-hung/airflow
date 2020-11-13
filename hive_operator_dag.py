from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.utils.dates import days_ago

from datetime import timedelta, datetime
import time
import pendulum
local_tz = pendulum.timezone("Asia/Bangkok")

default_args = {
    'owner': 'Hung',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 1, tzinfo=local_tz),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG(
    'hive_operator_dag',
    default_args=default_args,
    description='Test Hive Operator',
    schedule_interval=timedelta(days=1),
)


def sleep_func(ds, **kwargs):
    time.sleep(5)


t1_hive_create_table = HiveOperator(
    task_id='t1_hive_create_table',
    dag=dag,
    hive_cli_conn_id="hive_conn",
    hql="""
        CREATE TABLE IF NOT EXISTS db.table (x INT, y STRING) STORED AS PARQUET
        """
)
t2_python_sleep = PythonOperator(
    task_id='t2_python_sleep',
    provide_context=True,
    python_callable=sleep_func,
    dag=dag,
)
t3_hive_insert = HiveOperator(
    task_id='t3_hive_insert',
    dag=dag,
    hive_cli_conn_id="hive_conn",
    hql="""
        INSERT INTO db.table (x, y)
        VALUES (1, "First")
        """
)
t4_hive_drop_table = HiveOperator(
    task_id='t4_hive_drop_table',
    dag=dag,
    hive_cli_conn_id="hive_conn",
    hql="""
        DROP TABLE IF NOT EXISTS db.table PURGE
        """
)

t1_hive_create_table >> t2_python_sleep >> t3_hive_insert >> t4_hive_drop_table
