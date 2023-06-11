import os
import logging

from datetime import datetime

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from helpers import func_download_dataframe, func_load_dataframe

# -----------------------------------------------------------------------------

PROCESSED_DIR = '/tmp/data/processed/'


def concatenate_lists(list1, list2):
    return [*list1, *list2]


# -----------------------------------------------------------------------------

dag_cargar_fact_tables = DAG(
    "dag_cargar_fact_tables",
    schedule_interval="@yearly",
    start_date=datetime(2014, 1, 1),
    tags=['dw-training'],
)

task_start = DummyOperator(
    task_id = "task_start",
    dag=dag_cargar_fact_tables,
)

# -----------------------------------------------------------------------------

def func_load_fact_international_graduated(mysql_connection_id):

    fields_numero_egresados_internacional = ["year", "country", "num_graduated_M", "num_graduated_F", "num_graduated"]
    stage_numero_egresados_internacional = func_download_dataframe(fields=fields_numero_egresados_internacional, 
                                                                    table="stage_numero_egresados_internacional", 
                                                                    mysql_conn_id=mysql_connection_id)

    fields_porcentaje_egresados_internacional = ["year", "country", "percentage_graduated", "percentage_youth_graduated"]
    stage_porcentaje_egresados_internacional = func_download_dataframe(fields=fields_porcentaje_egresados_internacional, 
                                                                        table="stage_porcentaje_egresados_internacional", 
                                                                        mysql_conn_id=mysql_connection_id)

    stage_data = pd.merge(
        left=stage_numero_egresados_internacional,
        right=stage_porcentaje_egresados_internacional,
        how="outer",
        left_on=["year", "country"],
        right_on=["year", "country"]
    )

    dimm_pais = func_download_dataframe(fields=["id", "nombre_pais"], 
                                        table="dimm_pais", 
                                        mysql_conn_id=mysql_connection_id)

    fact_international_graduated = pd.merge(
        left=stage_data,
        right=dimm_pais,
        how="outer",
        left_on="country",
        right_on="nombre_pais"
    )

    diccionario_columnas = {
        "id": "id_country",
        "num_graduated_M": "num_graduated_male",
        "num_graduated_F": "num_graduated_female",
    }
    fact_international_graduated = fact_international_graduated.drop(columns=["country", "nombre_pais"])
    fact_international_graduated = fact_international_graduated.rename(columns=diccionario_columnas)

    # temp_file_filename = os.path.join(PROCESSED_DIR, "fact_international_graduated.csv")
    # fact_international_graduated.to_csv(temp_file_filename, index=False)
    func_load_dataframe(fact_international_graduated, "fact_international_graduated", mysql_connection_id)


task_load_fact_international_graduated = PythonOperator(
    python_callable= func_load_fact_international_graduated,
    task_id="task_load_fact_international_graduated",
    dag=dag_cargar_fact_tables,
    op_kwargs=dict(
        mysql_connection_id="conexion_mysql"
    )
)

# -----------------------------------------------------------------------------

def func_load_fact_situacion_laboral_egresados(mysql_connection_id):

    field_situacion_laboral_egresados = ["año", "pais", "tipo_universidad", "area_estudio", "sexo", "situacion_laboral", "cantidad"]
    stage_situacion_laboral_egresados = func_download_dataframe(fields=field_situacion_laboral_egresados, 
                                                                    table="stage_situacion_laboral_egresados", 
                                                                    mysql_conn_id=mysql_connection_id)
    
    # lista de campos que cambiar
    lista_dimm_tables = [
        {
            "nombre_tabla": "dimm_pais",
            "campo_id": ["id"],
            "nombres_campos": ["id_pais"],
            "campo_match_dimm": "nombre_pais",
            "campo_match_stage": "pais"
        },
        {
            "nombre_tabla": "dimm_tipo_universidad",
            "campo_id": ["id"],
            "nombres_campos": ["id_tipo_universidad"],
            "campo_match_dimm": "desc_tipo_universidad",
            "campo_match_stage": "tipo_universidad"
        },
        {
            "nombre_tabla": "dimm_sexo",
            "campo_id": ["id"],
            "nombres_campos": ["id_sexo"],
            "campo_match_dimm": "desc_sexo",
            "campo_match_stage": "sexo"
        },
        {
            "nombre_tabla": "dimm_situacion_laboral",
            "campo_id": ["id"],
            "nombres_campos": ["id_situacion_laboral"],
            "campo_match_dimm": "desc_situacion_laboral",
            "campo_match_stage": "situacion_laboral"
        },
        {
            "nombre_tabla": "dimm_rama_enseñanza",
            "campo_id": ["id"],
            "nombres_campos": ["id_area_estudio"],
            "campo_match_dimm": "nombre_rama",
            "campo_match_stage": "area_estudio"
        }
    ]

    for item in lista_dimm_tables:

        # fields_dims = item["campo_id"] + [item["campo_match_dimm"]]
        fields_dims = concatenate_lists(item["campo_id"], [item["campo_match_dimm"]])
        # rename_dims = item["nombres_campos"] + [item["campo_match_dimm"]]
        rename_dims = concatenate_lists(item["nombres_campos"], [item["campo_match_dimm"]])

        logging.info(fields_dims)
        logging.info(rename_dims)

        dimm_df = func_download_dataframe(fields=fields_dims, 
                                          table=item["nombre_tabla"], 
                                          mysql_conn_id=mysql_connection_id)
        
        rename_columns = dict(zip(fields_dims, rename_dims))
        dimm_df = dimm_df.rename(columns=rename_columns)

        stage_situacion_laboral_egresados = pd.merge(
            left=stage_situacion_laboral_egresados, 
            right=dimm_df,
            how="left",
            left_on=item["campo_match_stage"],
            right_on=item["campo_match_dimm"]
        )

        stage_situacion_laboral_egresados = stage_situacion_laboral_egresados.drop(
            columns=[item["campo_match_dimm"], item["campo_match_stage"]]
        )

    temp_file_filename = os.path.join(PROCESSED_DIR, "fact_situacion_laboral_egresados.csv")
    stage_situacion_laboral_egresados.to_csv(temp_file_filename, index=False)
    # func_load_dataframe(stage_situacion_laboral_egresados, "situacion_laboral_egresados", mysql_connection_id)


task_load_fact_situacion_laboral_egresados = PythonOperator(
    python_callable= func_load_fact_situacion_laboral_egresados,
    task_id="task_load_fact_situacion_laboral_egresados",
    dag=dag_cargar_fact_tables,
    op_kwargs=dict(
        mysql_connection_id="conexion_mysql"
    )
)

# -----------------------------------------------------------------------------

def func_load_fact_egresados_rama_enseñanza(mysql_connection_id):

    fields_egresados_universidad = [
        "año", 
        "pais", 
        "nombre_universidad",
        "rama_enseñanza", 
        "num_egresados"
        ]
    stage_egresados_universidad = func_download_dataframe(fields=fields_egresados_universidad, 
                                                            table="stage_egresados_universidad", 
                                                            mysql_conn_id=mysql_connection_id) 

    # lista de campos que cambiar
    lista_dimm_tables = [
        {
            "nombre_tabla": "dimm_pais",
            "campo_id": ["id"],
            "nombres_campos": ["id_pais"],
            "campo_match_dimm": "nombre_pais",
            "campo_match_stage": "pais"
        },
        {
            "nombre_tabla": "dimm_universidades",
            "campo_id": ["id"],
            "nombres_campos": ["id_universidad"],
            "campo_match_dimm": "nombre_universidad",
            "campo_match_stage": "nombre_universidad"
        },
        {
            "nombre_tabla": "dimm_rama_enseñanza",
            "campo_id": ["id"],
            "nombres_campos": ["id_rama_enseñanza"],
            "campo_match_dimm": "nombre_rama",
            "campo_match_stage": "rama_enseñanza"
        },
    ]

    for item in lista_dimm_tables:

        # fields_dims = item["campo_id"] + [item["campo_match_dimm"]]
        fields_dims = concatenate_lists(item["campo_id"], [item["campo_match_dimm"]])
        # rename_dims = item["nombres_campos"] + [item["campo_match_dimm"]]
        rename_dims = concatenate_lists(item["nombres_campos"], [item["campo_match_dimm"]])

        logging.info(fields_dims)
        logging.info(rename_dims)
        dimm_df = func_download_dataframe(fields=fields_dims, 
                                          table=item["nombre_tabla"], 
                                          mysql_conn_id=mysql_connection_id)
        
        rename_columns = dict(zip(fields_dims, rename_dims))
        dimm_df = dimm_df.rename(columns=rename_columns)

        stage_egresados_universidad = pd.merge(
            left=stage_egresados_universidad, 
            right=dimm_df,
            how="left",
            left_on=item["campo_match_stage"],
            right_on=item["campo_match_dimm"]
        )

        stage_egresados_universidad = stage_egresados_universidad.drop(
            columns=[item["campo_match_dimm"], item["campo_match_stage"]]
        )

    temp_file_filename = os.path.join(PROCESSED_DIR, "fact_situacion_laboral_egresados.csv")
    stage_egresados_universidad.to_csv(temp_file_filename, index=False)
    # func_load_dataframe(stage_egresados_universidad, "situacion_laboral_egresados", mysql_connection_id)


task_load_fact_egresados_rama_enseñanza = PythonOperator(
    python_callable= func_load_fact_egresados_rama_enseñanza,
    task_id="task_load_fact_egresados_rama_enseñanza",
    dag=dag_cargar_fact_tables,
    op_kwargs=dict(
        mysql_connection_id="conexion_mysql"
    )
)

# -----------------------------------------------------------------------------

def func_load_fact_egresados_niveles(mysql_connection_id):

    fields_egresados_niveles = [
        "año", 
        "pais", 
        "cod_ambito",
        "sexo",
        "edad",
        "num_egresados_nivel_1",
        "num_egresados_nivel_2",
        ]
    stage_egresados_niveles = func_download_dataframe(fields=fields_egresados_niveles, 
                                                            table="stage_egresados_niveles", 
                                                            mysql_conn_id=mysql_connection_id) 


    # lista de campos que cambiar
    lista_dimm_tables = [
        {
            "nombre_tabla": "dimm_pais",
            "campo_id": ["id"],
            "nombres_campos": ["id_pais"],
            "campo_match_dimm": "nombre_pais",
            "campo_match_stage": "pais"
        },
        {
            "nombre_tabla": "dimm_sexo",
            "campo_id": ["id"],
            "nombres_campos": ["id_sexo"],
            "campo_match_dimm": "desc_sexo",
            "campo_match_stage": "sexo"
        },
        {
            "nombre_tabla": "dimm_rango_edad",
            "campo_id": ["id"],
            "nombres_campos": ["id_rango_edad"],
            "campo_match_dimm": "desc_rango_edad",
            "campo_match_stage": "edad"
        },        
    ]

    for item in lista_dimm_tables:

        # fields_dims = item["campo_id"] + [item["campo_match_dimm"]]
        fields_dims = concatenate_lists(item["campo_id"], [item["campo_match_dimm"]])
        # rename_dims = item["nombres_campos"] + [item["campo_match_dimm"]]
        rename_dims = concatenate_lists(item["nombres_campos"], [item["campo_match_dimm"]])

        logging.info(fields_dims)
        logging.info(rename_dims)

        dimm_df = func_download_dataframe(fields=fields_dims, 
                                          table=item["nombre_tabla"], 
                                          mysql_conn_id=mysql_connection_id)
        
        rename_columns = dict(zip(fields_dims, rename_dims))
        dimm_df = dimm_df.rename(columns=rename_columns)

        stage_egresados_niveles = pd.merge(
            left=stage_egresados_niveles, 
            right=dimm_df,
            how="left",
            left_on=item["campo_match_stage"],
            right_on=item["campo_match_dimm"]
        )

        stage_egresados_niveles = stage_egresados_niveles.drop(
            columns=[item["campo_match_dimm"], item["campo_match_stage"]]
        )

    stage_egresados_niveles["num_egresados"] =  stage_egresados_niveles["num_egresados_nivel_1"] + stage_egresados_niveles["num_egresados_nivel_2"]

    diccionario_columnas = {
        "cod_ambito": "id_ambito"
    }
    stage_egresados_niveles = stage_egresados_niveles.rename(columns=diccionario_columnas)

    temp_file_filename = os.path.join(PROCESSED_DIR, "fact_stage_egresados_niveles.csv")
    stage_egresados_niveles.to_csv(temp_file_filename, index=False)
    # func_load_dataframe(stage_egresados_niveles, "situacion_laboral_egresados", mysql_connection_id)


task_load_fact_egresados_niveles = PythonOperator(
    python_callable= func_load_fact_egresados_niveles,
    task_id="task_load_fact_egresados_niveles",
    dag=dag_cargar_fact_tables,
    op_kwargs=dict(
        mysql_connection_id="conexion_mysql"
    )
)

# -----------------------------------------------------------------------------

task_start >> task_load_fact_international_graduated
task_start >> task_load_fact_situacion_laboral_egresados
task_start >> task_load_fact_egresados_rama_enseñanza
task_start >> task_load_fact_egresados_niveles