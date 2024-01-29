import airflow
import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


import os
from pathlib import Path
from airflow.configuration import conf
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

import json

from airflow import DAG, settings
from airflow.models import Connection


from common.utils import get_default_google_cloud_connection_id




default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

first_dag = DAG(
    dag_id='a_employee',
    default_args=default_args_dict,
    catchup=False,
)

task_one = BashOperator(
    task_id='get_spreadsheet',
    dag=first_dag,
    bash_command="wget -O /home/turgay/employee.csv https://drive.usercontent.google.com/download?id=1hRykFPs4wG-8itx2pOWy47wz3CkBfRpF&export=download&authuser=0",
       
)


def csv_filter():
    df=pd.read_csv('/home/turgay/employee.csv')
    df=df[df['MaritalStatus']=='Single']
    df.dropna(inplace=True)
    df.to_csv('/home/turgay/emp_filtered.csv')  



task_two = PythonOperator(
    task_id='convert_to_csv',
    dag=first_dag,
    python_callable=csv_filter,
    trigger_rule='all_success',
    depends_on_past=False,
)

def add_gcp_connection(**kwargs):
    """"Add a airflow connection for GCP"""
    new_conn = Connection(
        conn_id=get_default_google_cloud_connection_id(),
        conn_type='google_cloud_platform',
    )
    scopes = [
        "https://www.googleapis.com/auth/pubsub",
        "https://www.googleapis.com/auth/datastore",
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/devstorage.read_write",
        "https://www.googleapis.com/auth/logging.write",
        "https://www.googleapis.com/auth/cloud-platform",
    ]
    conn_extra = {
        "extra__google_cloud_platform__scope": ",".join(scopes),
        "extra__google_cloud_platform__project": "deprojectv2",
        "extra__google_cloud_platform__key_path": '/home/turgay/airflow/deprojectv2-1aab0291964f.json'
    }
    conn_extra_json = json.dumps(conn_extra)
    new_conn.set_extra(conn_extra_json)

    session = settings.Session()
    if not (session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first()):
        session.add(new_conn)
        session.commit()
    else:
        msg = '\n\tA connection with `conn_id`={conn_id} already exists\n'
        msg = msg.format(conn_id=new_conn.conn_id)
        print(msg)

task_three = LocalFilesystemToGCSOperator(
       task_id="local_to_gcs",
       src='/home/turgay/emp_filtered.csv',# PATH_TO_UPLOAD_FILE
       dst="emp_filtered.csv",# BUCKET_FILE_LOCATION
       bucket="projectv1_turgay",#using NO 'gs://' nor '/' at the end, only the project, folders, if any, in dst
       dag=first_dag,
        google_cloud_default=add_gcp_connection()
      
   )




task_four = BashOperator(
    task_id='load',
    dag=first_dag,
    bash_command="echo \"done\""
)

task_five = BashOperator(
    task_id='cleanup',
    dag=first_dag,
    bash_command="rm /opt/airflow/dags/{{ds_nodash}}_correct.csv /opt/airflow/dags/{{ds_nodash}}_correct_filtered.csv /opt/airflow/dags/{{ds_nodash}}.xlsx",
)

task_one >> task_two >> task_three >> task_four >> task_five
