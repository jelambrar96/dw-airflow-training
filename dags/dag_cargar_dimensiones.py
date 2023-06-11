import os

from datetime import datetime

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from helpers import func_download_dataframe, func_load_dataframe


PROCESSED_DIR = '/tmp/data/processed/'


def func_download_dimm_data(tables_field_tuple, dimension_table, dimension_name, mysql_connection_id):

    dimensional_data = func_download_dataframe(
        fields=[dimension_name], 
        table=dimension_table, 
        mysql_conn_id=mysql_connection_id, 
        distinct=True
    )
    temp_dimesion_name = "_{}".format(dimension_name)

    state_tables_list = []
    for item_table, item_field in tables_field_tuple:
        temporal_df = func_download_dataframe(
            fields=[item_field], 
            table=item_table, 
            mysql_conn_id=mysql_connection_id, 
            distinct=True
        )
        temporal_df = temporal_df.rename(columns={item_field: temp_dimesion_name})
        # state_tables_df = pd.concat(temporal_df, state_tables_df)
        state_tables_list.append(temporal_df)

    state_tables_df = pd.concat(state_tables_list)
    state_tables_df = state_tables_df.drop_duplicates(keep='first', subset=temp_dimesion_name)

    # temp_file_filename = os.path.join(PROCESSED_DIR, "state_tables_dimension_{}.csv".format(dimension_name))
    # state_tables_df.to_csv(temp_file_filename, index=False)

    # temp_file_filename = os.path.join(PROCESSED_DIR, "dimensional_data_dimension_{}.csv".format(dimension_name))
    # dimensional_data.to_csv(temp_file_filename, index=False)

    new_dimension_df = pd.merge(state_tables_df, dimensional_data, left_on=temp_dimesion_name, right_on=dimension_name, how='left')
    new_dimension_df = new_dimension_df[new_dimension_df[dimension_name].isnull()].drop([dimension_name], axis=1)
    new_dimension_df = new_dimension_df.rename(columns={temp_dimesion_name: dimension_name})

    # temp_file_filename = os.path.join(PROCESSED_DIR, "dimension_{}.csv".format(dimension_name))
    # new_dimension_df.to_csv(temp_file_filename, index=False)
    func_load_dataframe(new_dimension_df, dimension_table, mysql_connection_id)



def func_download_dimm_unique_table_stage(table_stage_in, fields_stage_in, dimension_table, dimension_names, mysql_connection_id):
    
    dimensional_data = func_download_dataframe(
        fields=dimension_names, 
        table=dimension_table, 
        mysql_conn_id=mysql_connection_id, 
        distinct=True
    )
    temp_dimension_names = [ "_{}".format(item) for item in dimension_names ]

    # datos from unique table 
    temporal_df = func_download_dataframe(
        fields=fields_stage_in, 
        table=table_stage_in, 
        mysql_conn_id=mysql_connection_id, 
        distinct=True
    )

    diccionario_columnas = dict(zip(fields_stage_in, temp_dimension_names))
    temporal_df = temporal_df.rename(columns=diccionario_columnas)

    new_dimension_df = pd.merge(temporal_df, dimensional_data, left_on=temp_dimension_names, right_on=dimension_names, how='left')
    new_dimension_df = new_dimension_df[new_dimension_df[dimension_names].isnull()].drop(dimension_names, axis=1)
    
    dict_new_dimension_columns = dict(zip(temp_dimension_names, dimension_names))
    new_dimension_df = new_dimension_df.rename(columns=dict_new_dimension_columns)

    pass

# -----------------------------------------------------------------------------

dag_cargar_dimensiones = DAG(
    "dag_cargar_dimensiones",
    schedule_interval="@yearly",
    start_date=datetime(2014, 1, 1),
    tags=['dw-training'],
)

task_start = DummyOperator(
    task_id = "task_start",
    dag=dag_cargar_dimensiones,
)


# dimension pais
task_load_dimension_pais = PythonOperator(
    python_callable= func_download_dimm_data,
    task_id="task_load_dimension_pais",
    dag=dag_cargar_dimensiones,
    op_kwargs=dict(
        tables_field_tuple=[
            ("stage_numero_egresados_internacional", "country"),
            ("stage_porcentaje_egresados_internacional", "country"),
            ("stage_situacion_laboral_egresados", "pais"),
            ("stage_egresados_universidad", "pais"),
        ],
        dimension_table="dimm_pais",
        dimension_name="nombre_pais",
        mysql_connection_id="conexion_mysql"
    ),
)

# dimension sexo 
task_load_dimension_sexo = PythonOperator(
    python_callable= func_download_dimm_data,
    task_id="task_load_dimension_sexo",
    dag=dag_cargar_dimensiones,
    op_kwargs=dict(
        tables_field_tuple=[
            ("stage_situacion_laboral_egresados", "sexo"),
            ("stage_egresados_niveles", "sexo"),
        ],
        dimension_table="dimm_sexo",
        dimension_name="desc_sexo",
        mysql_connection_id="conexion_mysql"
    ),
)

# dimension tipo_universidad
task_load_dimension_tipo_universidad = PythonOperator(
    python_callable= func_download_dimm_data,
    task_id="task_load_dimension_tipo_universidad",
    dag=dag_cargar_dimensiones,
    op_kwargs=dict(
        tables_field_tuple=[
            ("stage_egresados_universidad", "tipo_universidad"),
            ("stage_situacion_laboral_egresados", "tipo_universidad"),
        ],
        dimension_table="dimm_tipo_universidad",
        dimension_name="desc_tipo_universidad",
        mysql_connection_id="conexion_mysql"
    ),
)

# dimension universidades
task_load_dimension_universidades = PythonOperator(
    python_callable= func_download_dimm_unique_table_stage,
    task_id="task_load_dimension_universidades",
    dag=dag_cargar_dimensiones,
    op_kwargs=dict(
        table_stage_in="stage_egresados_universidad", 
        fields_stage_in=["nombre_universidad", "tipo_universidad", "modalidad"], 
        dimension_table="dimm_universidades", 
        dimension_names=["nombre_universidad", "tipo_universidad", "modalidad"], 
        mysql_connection_id="conexion_mysql"
    ),
)

# dimension rama_ensenanza
task_load_dimension_rama_ensenanza = PythonOperator(
    python_callable= func_download_dimm_data,
    task_id="task_load_dimension_rama_ensenanza",
    dag=dag_cargar_dimensiones,
    op_kwargs=dict(
        tables_field_tuple=[
            ("stage_egresados_universidad", "rama_enseñanza"),
        ],
        dimension_table="dimm_rama_enseñanza",
        dimension_name="nombre_rama",
        mysql_connection_id="conexion_mysql"
    ),
)

# dimension ambito ensenanza
task_load_dimension_ambito_ensenanza = PythonOperator(
    python_callable= func_download_dimm_unique_table_stage,
    task_id="task_load_dimension_ambito_ensenanza",
    dag=dag_cargar_dimensiones,
    op_kwargs=dict(
        table_stage_in="stage_ramas_conocimiento", 
        fields_stage_in=["codigo_rama_4", "nombre_rama_4", "codigo_rama_1", "nombre_rama_1"], 
        dimension_table="dimm_ambito_enseñanza", 
        dimension_names=["id", "desc_ambito", "id_rama", "nombre_rama"], 
        mysql_connection_id="conexion_mysql"
    ),
)

# dimension situacion laboral 
task_load_dimension_situacion_laboral= PythonOperator(
    python_callable= func_download_dimm_data,
    task_id="task_load_dimension_situacion_laboral",
    dag=dag_cargar_dimensiones,
    op_kwargs=dict(
        tables_field_tuple=[
            ("stage_situacion_laboral_egresados", "situacion_laboral"),
        ],
        dimension_table="dimm_situacion_laboral",
        dimension_name="desc_situacion_laboral",
        mysql_connection_id="conexion_mysql"
    ),
)

# dimension rango edad
task_load_dimension_rango_edad = PythonOperator(
    python_callable= func_download_dimm_data,
    task_id="task_load_dimension_rango_edad",
    dag=dag_cargar_dimensiones,
    op_kwargs=dict(
        tables_field_tuple=[
            ("stage_egresados_niveles", "edad"),
        ],
        dimension_table="dimm_rango_edad",
        dimension_name="desc_rango_edad",
        mysql_connection_id="conexion_mysql"
    ),
)

task_start >> task_load_dimension_pais
task_start >> task_load_dimension_sexo
task_start >> task_load_dimension_tipo_universidad
task_start >> task_load_dimension_universidades
task_start >> task_load_dimension_rama_ensenanza
task_start >> task_load_dimension_ambito_ensenanza
task_start >> task_load_dimension_situacion_laboral
task_start >> task_load_dimension_rango_edad
