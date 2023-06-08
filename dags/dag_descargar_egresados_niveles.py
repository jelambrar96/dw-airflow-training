from datetime import datetime

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

from helpers import func_bulk_load_sql


CSV_FILE = "/tmp/data/raw/grad_5sc.csv"
OUT_CSV_FILE = "/tmp/data/processed/grad_5sc.csv"



def func_detect_sexo(x):
    tempx = x.lower()
    if "mujer" in tempx:
        return "Femenino"
    elif "hombre" in tempx:
        return "Masculino"
    else :
        return x

# ------------------------------------------------------------------------------------

def func_preprocessing_csv(in_file, out_file):
    raw_df = pd.read_csv(in_file, encoding="latin_1", delimiter=";", encoding_errors="ignore")
    diccionario_columnas = {
        "SEXO": "sexo",
        "EDAD": "edad",
        "NUM_EGR_NV1": "num_egresados_nivel_1", 
        "NUM_EGR_NV2": "num_egresados_nivel_2", 
        "COD_AMBITO": "cod_ambito_full"
    }
    raw_df = raw_df.rename(columns=diccionario_columnas)
    raw_df["cod_ambito"] = raw_df["cod_ambito_full"].apply(lambda x: x[:12])
    raw_df["ambito"] = raw_df["cod_ambito_full"].apply(lambda x: x[15:])
    raw_df["pais"] = "Spain"
    raw_df["sexo"] = raw_df["sexo"].apply(lambda x: func_detect_sexo(x))
    raw_df["año"] = 2017
    raw_df = raw_df.drop(columns=["cod_ambito_full"])
    raw_df.to_csv(out_file,index=False)


# ------------------------------------------------------------------------------------

dag_descargar_egresados_niveles = DAG(
    "dag_descargar_egresados_niveles",
    schedule_interval="@yearly",
    start_date=datetime(2014, 1, 1),
    tags=['dw-training'],
)

# ------------------------------------------------------------------------------------

task_preprocessing_csv = PythonOperator(
    task_id = "task_preprocessing_csv",
    python_callable= func_preprocessing_csv,
    dag=dag_descargar_egresados_niveles,
    op_kwargs=dict(
        in_file=CSV_FILE,
        out_file=OUT_CSV_FILE,
    ),
)


task_load_to_database = PythonOperator(
    task_id='task_load_to_database',
    provide_context=True,
    dag=dag_descargar_egresados_niveles,
    python_callable=func_bulk_load_sql,
    op_kwargs=dict(
        csv_file=OUT_CSV_FILE,
        table_name='stage_egresados_niveles',
        mysql_conn_id='conexion_mysql',
    ),
)

# ------------------------------------------------------------------------------------

task_preprocessing_csv >> task_load_to_database