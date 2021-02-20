from airflow import DAG
from airflow.operators import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
import requests
import logging
import psycopg2

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

def etl():
    logging.info("ctas started")
    cur = get_Redshift_connection()
    schema = 'goldenboyy0524'
    table = 'ctas_test'
    cur.execute("BEGIN;")
    cur.execute("DROP TABLE IF EXISTS {}.{};".format(schema, table))
    cur.execute("CREATE TABLE {}.{} AS SELECT * FROM raw_data.user_session_channel WHERE channel = 'Google';".format(schema, table))
    cur.execute("COMMIT")
    logging.info("ctas done")

dag_second_assignment = DAG(
	dag_id = 'ctas_test',
	start_date = datetime(2021,2,4), # 날짜가 미래인 경우 실행이 안됨
	schedule_interval = '0 2 * * *')  # 적당히 조절

task = PythonOperator(
	task_id = 'perform_etl',
	python_callable = etl,
	dag = dag_second_assignment)

