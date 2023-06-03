import os
import csv
import logging

from datetime import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator

from helpers import preprocesar_archivo_situacion_laboral, cargar_archivo_situacion_laboral
from helpers import func_bulk_load_sql


DATA_DIRECTORY = "/tmp/data/raw/"
FILE = '03003.xlsx'
CSV_FILE = '/tmp/data/processed/03003.csv'

workflow = DAG(
    "dag_descargar_archivo_situacion_laboral_egresados",
    schedule_interval="@yearly",
    start_date=datetime(2014, 1, 1),
    tags=['dw-training'],
)

with workflow:

    preprocessing_task = PythonOperator(
        task_id="preprocesar_archivo_situacion_laboral_egresados",
        python_callable=preprocesar_archivo_situacion_laboral,
        op_kwargs=dict(
            file=FILE,
            directory=DATA_DIRECTORY,
        ),
    )

    upload_task = PythonOperator(
        task_id="cargar_archivo_situacion_laboral_egresados",
        python_callable=cargar_archivo_situacion_laboral,
    )

    load_to_database = PythonOperator(
        task_id='load_to_database',
        provide_context=True,
        python_callable=func_bulk_load_sql,
        dag=workflow ,
        op_kwargs=dict(
            csv_file=CSV_FILE,
            table_name='stage_situacion_laboral_egresados',
            mysql_conn_id='conexion_mysql',
        ),
    )

    preprocessing_task >> upload_task >> load_to_database
