from datetime import datetime

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from helpers import func_bulk_load_sql


CSV_FILE_SEGR2 = "/tmp/data/raw/SEGR2.csv"
OUT_CSV_FILE_SEGR2 = "/tmp/data/processed/SEGR2.csv"

CSV_FILE_SEGR1 = "/tmp/data/raw/SEGR1.csv"
OUT_CSV_FILE_SEGR1 = "/tmp/data/processed/SEGR1.csv"

# ------------------------------------------------------------------------------------

def func_preprocessing_csv(in_file, out_file):
    
    raw_df = pd.read_csv(in_file, encoding="latin_1", delimiter=";", encoding_errors="ignore")
    
    diccionario_columnas = {
        "TIPO_UNIVERSIDAD": "tipo_universidad", 
        "MODALIDAD": "modalidad", 
        "UNIVERSIDAD": "nombre_universidad", 
        "RAMA_ENSEÑANZA": "rama_enseñanza", 
        "EGR_C16_17": "2017", 
        "EGR_C15_16": "2016", 
        "EGR_C14_15": "2015", 
        "EGR_C13_14": "2014", 
        "EGR_C12_13": "2013", 
        "EGR_C11_12": "2012", 
        "EGR_C10_11": "2011", 
        "EGR_C09_10": "2010",
    }
    raw_df = raw_df.rename(columns=diccionario_columnas)

    df = raw_df.melt(
        id_vars=["tipo_universidad", "modalidad", "nombre_universidad", "rama_enseñanza"], 
        value_vars=["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017"]
        )

    diccionario_columnas_df = {
        "variable": "año",
        "value": "num_egresados"
    }
    df = df.rename(columns=diccionario_columnas_df)
    df["pais"] = "Spain"
    df.to_csv(out_file,index=False)

# ------------------------------------------------------------------------------------

dag_descargar_egresados_universidad = DAG(
    "dag_descargar_egresados_universidad",
    schedule_interval="@yearly",
    start_date=datetime(2014, 1, 1),
    tags=['dw-training'],
)

# ------------------------------------------------------------------------------------

task_start = DummyOperator(
    task_id = "task_start",
    dag=dag_descargar_egresados_universidad,
)


task_preprocessing_csv_segr2 = PythonOperator(
    task_id = "task_preprocessing_csv_segr2",
    python_callable= func_preprocessing_csv,
    dag=dag_descargar_egresados_universidad,
    op_kwargs=dict(
        in_file=CSV_FILE_SEGR2,
        out_file=OUT_CSV_FILE_SEGR2,
    ),
)


task_load_to_database_csv_segr2 = PythonOperator(
    task_id='task_load_to_database_csv_segr2',
    provide_context=True,
    dag=dag_descargar_egresados_universidad,
    python_callable=func_bulk_load_sql,
    op_kwargs=dict(
        csv_file=OUT_CSV_FILE_SEGR2,
        table_name='stage_egresados_universidad',
        mysql_conn_id='conexion_mysql',
    ),
)


task_preprocessing_csv_segr1 = PythonOperator(
    task_id = "task_preprocessing_csv_segr1",
    python_callable= func_preprocessing_csv,
    dag=dag_descargar_egresados_universidad,
    op_kwargs=dict(
        in_file=CSV_FILE_SEGR1,
        out_file=OUT_CSV_FILE_SEGR1,
    ),
)


task_load_to_database_csv_segr1 = PythonOperator(
    task_id='task_load_to_database_csv_segr1',
    provide_context=True,
    dag=dag_descargar_egresados_universidad,
    python_callable=func_bulk_load_sql,
    op_kwargs=dict(
        csv_file=OUT_CSV_FILE_SEGR1,
        table_name='stage_egresados_universidad',
        mysql_conn_id='conexion_mysql',
    ),
)


# ------------------------------------------------------------------------------------

task_start >> task_preprocessing_csv_segr1 >> task_load_to_database_csv_segr1
task_start >> task_preprocessing_csv_segr2 >> task_load_to_database_csv_segr2
