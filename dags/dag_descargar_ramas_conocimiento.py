from datetime import datetime

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

from helpers import func_bulk_load_sql


CSV_FILE = "/tmp/data/raw/ISCED_2013.csv"
OUT_CSV_FILE = "/tmp/data/processed/ISCED_2013.csv"


# ------------------------------------------------------------------------------------

def func_preprocessing_csv(in_file, out_file):
    raw_df = pd.read_csv(in_file, encoding="latin_1", delimiter=";", encoding_errors="ignore", dtype=str)
    diccionario_columnas = {
        "COD_RAMA": "codigo_rama_1",
        "NOM_RAMA": "nombre_rama_1",
        "COD_RAMA_N2": "codigo_rama_2",
        "NOM_RAMA_N2": "nombre_rama_2",
        "COD_RAMA_N3": "codigo_rama_3",
        "NOM_RAMA_N3": "nombre_rama_3",
        "COD_RAMA_N4": "codigo_rama_4",
        "NOM_RAMA_N4": "nombre_rama_4",
        "COD_RAMA_N5": "codigo_rama_5",
        "NOM_RAMA_N5": "nombre_rama_5"
    }
    raw_df = raw_df.rename(columns=diccionario_columnas)
    columnas_raw_df = [ item for item in raw_df.columns if "codigo" in item]
    for item in columnas_raw_df:
        raw_df[item] = "C" + raw_df[item]
    raw_df.to_csv(out_file,index=False)


# ------------------------------------------------------------------------------------

dag_descargar_ramas_conocimiento = DAG(
    "dag_descargar_ramas_conocimiento",
    schedule_interval="@yearly",
    start_date=datetime(2014, 1, 1),
    tags=['dw-training'],
)

# ------------------------------------------------------------------------------------

task_preprocessing_csv = PythonOperator(
    task_id = "task_preprocessing_csv",
    python_callable= func_preprocessing_csv,
    dag=dag_descargar_ramas_conocimiento,
    op_kwargs=dict(
        in_file=CSV_FILE,
        out_file=OUT_CSV_FILE,
    ),
)


task_load_to_database = PythonOperator(
    task_id='task_load_to_database',
    provide_context=True,
    dag=dag_descargar_ramas_conocimiento,
    python_callable=func_bulk_load_sql,
    op_kwargs=dict(
        csv_file=OUT_CSV_FILE,
        table_name='stage_ramas_conocimiento',
        mysql_conn_id='conexion_mysql',
    ),
)

# ------------------------------------------------------------------------------------

task_preprocessing_csv >> task_load_to_database