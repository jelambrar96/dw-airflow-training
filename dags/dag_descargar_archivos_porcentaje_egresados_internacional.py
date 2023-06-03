import os
import csv
import logging

from datetime import datetime

import pandas as pd

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook

from helpers import func_bulk_load_sql

XLSX_FILE = "/tmp/data/raw/educ_uoe_grad05.xlsx"
CSV_FILE = "/tmp/data/processed/educ_uoe_grad05.csv"


def func_get_data_from_sheet(in_file, sheet_value=0):

    raw_df = pd.read_excel(
        in_file,
        header=10,
        skiprows=lambda x: x in range(23, 27),
        usecols='A:F',
        sheet_name=sheet_value,
        index_col=None,
    )

    raw_df = raw_df.replace(":", 0)
    raw_df = raw_df.astype(int, errors='ignore')

    return raw_df


def func_unpivot_tables(raw_df):
    id_vars=["GEO/TIME"]
    df = pd.melt(raw_df, id_vars=id_vars)
    return df


def func_preprocessing_xlsx(in_file: str, out_file: str):
      
    # hoja 1
    raw_m_data = func_get_data_from_sheet(in_file=in_file, sheet_value=0)
    raw_m_data = func_unpivot_tables(raw_m_data)
    diccionario_columnas = {
        "value": "percentage_graduated", 
        "variable": "year", 
        "GEO/TIME": "country"
    }
    raw_m_data = raw_m_data.rename(columns=diccionario_columnas)
    
    # hoja 2
    raw_f_data = func_get_data_from_sheet(in_file=in_file, sheet_value=1)
    raw_f_data = func_unpivot_tables(raw_f_data)
    diccionario_columnas = {
        "value": "percentage_youth_graduated", 
        "variable": "year", 
        "GEO/TIME": "country"
    }
    raw_f_data = raw_f_data.rename(columns=diccionario_columnas)
    
    # merge all documents
    new_df = pd.merge(
        raw_f_data, 
        raw_m_data, 
        how="outer", 
        left_on=["country", "year"], 
        right_on=["country", "year"]
    )

    new_df = new_df.replace("Germany (until 1990 former territory of the FRG)", "Germany")

    new_df.to_csv(out_file, index=False)


# ------------------------------------------------------------------------------------

dag_descargar_archivos_porcentaje_egresados_internacional = DAG(
    "dag_descargar_archivos_porcentaje_egresados_internacional",
    schedule_interval="@yearly",
    start_date=datetime(2014, 1, 1),
    tags=['dw-training'],
)

# ------------------------------------------------------------------------------------

task_preprocessing_xlsx = PythonOperator(
    task_id = "task_preprocessing_xlsx",
    python_callable= func_preprocessing_xlsx,
    dag=dag_descargar_archivos_porcentaje_egresados_internacional,
    op_kwargs=dict(
        in_file=XLSX_FILE,
        out_file=CSV_FILE,
    ),
)

task_load_to_database = PythonOperator(
    task_id='task_load_to_database',
    provide_context=True,
    dag=dag_descargar_archivos_porcentaje_egresados_internacional,
    python_callable=func_bulk_load_sql,
    op_kwargs=dict(
        csv_file=CSV_FILE,
        table_name='stage_porcentaje_egresados_internacional',
        mysql_conn_id='conexion_mysql',
    ),
)

# ------------------------------------------------------------------------------------

task_preprocessing_xlsx >> task_load_to_database