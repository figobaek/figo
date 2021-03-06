# -*- coding: utf-8 -*-


from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

dag = DAG (
    dag_id = 'my_4th_assignment',
    start_date = datetime(2021,2,4),
    schedule_interval = '0 4 * * *'
)
    
import requests

def extract(url):
    f = requests.get(link)
    return (f.text)

def transform(text):
    lines = text.split("\n")
    lines = lines[1:]
    return lines


def load(lines):
    cur = get_Redshift_connection()
    sql = "BEGIN; DROP TABLE goldenboyy0524.name_gender; CREATE TABLE goldenboyy0524.name_gender ( name varchar(32), gender varchar(8));"
    try:
      for r in lines:
          if r != '':
              (name, gender) = r.split(",")
              print(name, "-", gender)
              sql += "INSERT INTO goldenboyy0524.name_gender VALUES ('{name}', '{gender}');".format(name=name, gender=gender)
      sql += "COMMIT;"
    except:
      sql += "ROLLBACK;"
    print(sql)
    cur.execute(sql)


def ETL():
  link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
  data = extract(link)
  lines = transform(data)
  load(lines)


ETL_execute = PythonOperator(
    task_id = 'ETL_process',
    python_callable = ETL,
    dag = dag
)

ETL_execute
