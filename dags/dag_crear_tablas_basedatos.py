# dag_crear_tablas_basedatos.py
import os

from datetime import datetime

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.dummy import DummyOperator

MIGRATION_FILE = "/opt/airflow/migrations/create_tables.sql"
MIGRATION_DIR = "/opt/airflow/migrations/"

dag = DAG(
    "dag_crear_tablas_basedatos",
    schedule_interval=None,
    start_date=datetime(2014, 1, 1),
)


task_start = DummyOperator(task_id="task_start")


# definiendo las tareas que eliminan y generan las tablas

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

query_drop_stage_numero_egresados_internacional = """
DROP TABLE IF EXISTS stage_numero_egresados_internacional;
"""
task_drop_stage_numero_egresados_internacional = MySqlOperator(
    task_id="task_drop_stage_numero_egresados_internacional",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_stage_numero_egresados_internacional,
    dag=dag,
)

query_create_stage_numero_egresados_internacional = """
CREATE TABLE IF NOT EXISTS stage_numero_egresados_internacional (
    year INT,
    country VARCHAR(255),
    num_graduated_M INT,
    num_graduated_F INT,
    num_graduated INT
);
"""
task_create_stage_numero_egresados_internacional = MySqlOperator(
    task_id="task_create_stage_numero_egresados_internacional",
    mysql_conn_id='conexion_mysql',
    sql=query_create_stage_numero_egresados_internacional,
    dag=dag,
)

# -----------------------------------------------------------------------------

query_drop_stage_porcentaje_egresados_internacional = """
DROP TABLE IF EXISTS stage_porcentaje_egresados_internacional;
"""
task_drop_stage_porcentaje_egresados_internacional = MySqlOperator(
    task_id="task_drop_stage_porcentaje_egresados_internacional",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_stage_porcentaje_egresados_internacional,
    dag=dag,
)

query_create_stage_porcentaje_egresados_internacional = """
CREATE TABLE IF NOT EXISTS stage_porcentaje_egresados_internacional (
    year INT,
    country VARCHAR(255),
    percentage_graduated FLOAT,
    percentage_youth_graduated FLOAT
);
"""
task_create_stage_porcentaje_egresados_internacional = MySqlOperator(
    task_id="task_create_stage_porcentaje_egresados_internacional",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_stage_porcentaje_egresados_internacional,
    dag=dag,
)

# -----------------------------------------------------------------------------

query_drop_stage_situacion_laboral_egresados = """
DROP TABLE IF EXISTS stage_situacion_laboral_egresados;
"""
task_drop_stage_situacion_laboral_egresados = MySqlOperator(
    task_id="task_drop_stage_situacion_laboral_egresados",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_stage_situacion_laboral_egresados,
    dag=dag,
)

query_create_stage_situacion_laboral_egresados = """
CREATE TABLE IF NOT EXISTS stage_situacion_laboral_egresados (
    año INT,
    pais VARCHAR(255),
    tipo_universidad VARCHAR(255),
    area_estudio VARCHAR(255),
    sexo VARCHAR(255),
    situacion_laboral VARCHAR(255),
	cantidad INT
);
"""
task_create_stage_situacion_laboral_egresados = MySqlOperator(
    task_id="task_create_stage_situacion_laboral_egresados",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_stage_situacion_laboral_egresados,
    dag=dag,
)

# -----------------------------------------------------------------------------

query_drop_stage_egresados_niveles = """
DROP TABLE IF EXISTS stage_egresados_niveles;
"""
task_drop_stage_egresados_niveles = MySqlOperator(
    task_id="task_drop_stage_egresados_niveles",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_stage_egresados_niveles,
    dag=dag,
)

query_create_stage_egresados_niveles = """
CREATE TABLE IF NOT EXISTS stage_egresados_niveles (
    año INT,
    pais VARCHAR(255),
    cod_ambito VARCHAR(255),
    ambito VARCHAR(255),
    sexo VARCHAR(255),
    edad VARCHAR(255),
    num_egresados_nivel_1 INT,
    num_egresados_nivel_2 INT
);
"""
task_create_stage_egresados_niveles = MySqlOperator(
    task_id="task_create_stage_egresados_niveles",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_stage_egresados_niveles,
    dag=dag,
)

# -----------------------------------------------------------------------------

query_drop_stage_egresados_universidad = """
DROP TABLE IF EXISTS stage_egresados_universidad;
"""
task_drop_stage_egresados_universidad = MySqlOperator(
    task_id="task_drop_stage_egresados_universidad",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_stage_egresados_universidad,
    dag=dag,
)

query_create_stage_egresados_universidad = """
CREATE TABLE IF NOT EXISTS stage_egresados_universidad (
    año INT,
    pais VARCHAR(255),
    nombre_universidad VARCHAR(255),
    tipo_universidad VARCHAR(255),
    modalidad VARCHAR(255),
    rama_enseñanza VARCHAR(255),
    num_egresados INT
);
"""
task_create_stage_egresados_universidad = MySqlOperator(
    task_id="task_create_stage_egresados_universidad",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_stage_egresados_universidad,
    dag=dag,
)

# -----------------------------------------------------------------------------

query_drop_stage_ramas_conocimiento = """
DROP TABLE IF EXISTS stage_ramas_conocimiento;
"""
task_drop_stage_ramas_conocimiento = MySqlOperator(
    task_id="task_drop_stage_ramas_conocimiento",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_stage_ramas_conocimiento,
    dag=dag,
)

query_create_stage_ramas_conocimiento = """
CREATE TABLE IF NOT EXISTS stage_ramas_conocimiento (
	codigo_rama_1 VARCHAR(255),
    nombre_rama_1 VARCHAR(255),
    codigo_rama_2 VARCHAR(255),
    nombre_rama_2 VARCHAR(255),
    codigo_rama_3 VARCHAR(255),
    nombre_rama_3 VARCHAR(255),
    codigo_rama_4 VARCHAR(255),
    nombre_rama_4 VARCHAR(255),
    codigo_rama_5 VARCHAR(255),
    nombre_rama_5 VARCHAR(255)
);
"""
task_create_stage_ramas_conocimiento = MySqlOperator(
    task_id="task_create_stage_ramas_conocimiento",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_stage_ramas_conocimiento,
    dag=dag,
)

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

query_drop_dimm_pais = """
DROP TABLE IF EXISTS dimm_pais;
"""
task_drop_dimm_pais = MySqlOperator(
    task_id="task_drop_dimm_pais",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_dimm_pais,
    dag=dag,
)

query_create_dimm_pais = """
CREATE TABLE IF NOT EXISTS dimm_pais (
	id INTEGER AUTO_INCREMENT PRIMARY KEY,
	nombre_pais VARCHAR(255) NOT NULL
);
"""
task_create_dimm_pais = MySqlOperator(
    task_id="task_create_dimm_pais",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_dimm_pais,
    dag=dag,
)

# -----------------------------------------------------------------------------

query_drop_dimm_sexo = """
DROP TABLE IF EXISTS dimm_sexo;
"""
task_drop_dimm_sexo = MySqlOperator(
    task_id="task_drop_dimm_sexo",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_dimm_sexo,
    dag=dag,
)

query_create_dimm_sexo = """
CREATE TABLE IF NOT EXISTS dimm_sexo (
	id INTEGER AUTO_INCREMENT PRIMARY KEY,
	desc_sexo VARCHAR(255) NOT NULL
);
"""
task_create_dimm_sexo = MySqlOperator(
    task_id="task_create_dimm_sexo",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_dimm_sexo,
    dag=dag,
)

# -----------------------------------------------------------------------------

query_drop_dimm_tipo_universidad = """
DROP TABLE IF EXISTS dimm_tipo_universidad;
"""
task_drop_dimm_tipo_universidad = MySqlOperator(
    task_id="task_drop_dimm_tipo_universidad",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_dimm_tipo_universidad,
    dag=dag,
)

query_create_dimm_tipo_universidad = """
CREATE TABLE IF NOT EXISTS dimm_tipo_universidad (
	id INTEGER AUTO_INCREMENT PRIMARY KEY,
	desc_tipo_universidad VARCHAR(255)
);
"""
task_create_dimm_tipo_universidad = MySqlOperator(
    task_id="task_create_dimm_tipo_universidad",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_dimm_tipo_universidad,
    dag=dag,
)

# -----------------------------------------------------------------------------

query_drop_dimm_universidades = """
DROP TABLE IF EXISTS dimm_universidades;
"""
task_drop_dimm_universidades = MySqlOperator(
    task_id="task_drop_dimm_universidades",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_dimm_universidades,
    dag=dag,
)

query_create_dimm_universidades = """
CREATE TABLE IF NOT EXISTS dimm_universidades (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    nombre_universidad VARCHAR(255),
    tipo_universidad VARCHAR(255),
    modalidad VARCHAR(255)
);
"""
task_create_dimm_universidades = MySqlOperator(
    task_id="task_create_dimm_universidades",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_dimm_universidades,
    dag=dag,
)

# -----------------------------------------------------------------------------

query_drop_dimm_rama_ensenanza = """
DROP TABLE IF EXISTS dimm_rama_enseñanza;
"""
task_drop_dimm_rama_ensenanza = MySqlOperator(
    task_id="task_drop_dimm_rama_ensenanza",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_dimm_rama_ensenanza,
    dag=dag,
)

query_create_dimm_rama_ensenanza = """
CREATE TABLE IF NOT EXISTS dimm_rama_enseñanza (
	id INTEGER AUTO_INCREMENT PRIMARY KEY,
	nombre_rama VARCHAR(255)
);
"""
task_create_dimm_rama_ensenanza = MySqlOperator(
    task_id="task_create_dimm_rama_ensenanza",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_dimm_rama_ensenanza,
    dag=dag,
)

# -----------------------------------------------------------------------------

query_drop_dimm_ambito_ensenanza = """
DROP TABLE IF EXISTS dimm_ambito_enseñanza;
"""
task_drop_dimm_ambito_ensenanza = MySqlOperator(
    task_id="task_drop_dimm_ambito_ensenanza",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_dimm_ambito_ensenanza,
    dag=dag,
)

query_create_dimm_ambito_ensenanza = """
CREATE TABLE IF NOT EXISTS dimm_ambito_enseñanza (
	id VARCHAR(255),
    desc_ambito VARCHAR(255),
    id_rama VARCHAR(255),
	nombre_rama VARCHAR(255)
);
"""
task_create_dimm_ambito_ensenanza = MySqlOperator(
    task_id="task_create_dimm_ambito_ensenanza",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_dimm_ambito_ensenanza,
    dag=dag,
)

# -----------------------------------------------------------------------------

query_drop_dimm_situacion_laboral = """
DROP TABLE IF EXISTS dimm_situacion_laboral;
"""
task_drop_dimm_situacion_laboral = MySqlOperator(
    task_id="task_drop_dimm_situacion_laboral",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_dimm_situacion_laboral,
    dag=dag,
)

query_create_dimm_situacion_laboral = """
CREATE TABLE IF NOT EXISTS dimm_situacion_laboral (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    desc_situacion_laboral VARCHAR(255)
);
"""
task_create_dimm_situacion_laboral = MySqlOperator(
    task_id="task_create_dimm_situacion_laboral",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_dimm_situacion_laboral,
    dag=dag,
)

# -----------------------------------------------------------------------------

query_drop_dimm_edad = """
DROP TABLE IF EXISTS dimm_rango_edad;
"""
task_drop_dimm_edad = MySqlOperator(
    task_id="task_drop_dimm_edad",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_dimm_edad,
    dag=dag,
)

query_create_dimm_edad = """
CREATE TABLE IF NOT EXISTS dimm_rango_edad (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    desc_rango_edad VARCHAR(255)
);
"""
task_create_dimm_edad = MySqlOperator(
    task_id="task_create_dimm_edad",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_dimm_edad,
    dag=dag,
)


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

query_drop_fact_international_graduated = """
DROP TABLE IF EXISTS fact_international_graduated;
"""
task_drop_fact_international_graduated = MySqlOperator(
    task_id="task_drop_fact_international_graduated",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_fact_international_graduated,
    dag=dag,
)

query_create_fact_international_graduated = """
CREATE TABLE IF NOT EXISTS fact_international_graduated (
	year INT NOT NULL,
	id_country INT,
	num_graduated_male INT,
	num_graduated_female INT,
	num_graduated INT,
	percentage_graduated FLOAT,
	percentage_youth_graduated FLOAT
);
"""
task_create_fact_international_graduated = MySqlOperator(
    task_id="task_create_fact_international_graduated",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_fact_international_graduated,
    dag=dag,
)

# -----------------------------------------------------------------------------

query_drop_fact_situacion_laboral_egresados = """
DROP TABLE IF EXISTS fact_situacion_laboral_egresados;
"""
task_drop_fact_situacion_laboral_egresados = MySqlOperator(
    task_id="task_drop_fact_situacion_laboral_egresados",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_fact_situacion_laboral_egresados,
    dag=dag,
)

query_create_fact_situacion_laboral_egresados = """
CREATE TABLE IF NOT EXISTS fact_situacion_laboral_egresados (
    año INT NOT NULL,
    id_pais INT,
    id_tipo_universidad INT,
    id_area_estudio VARCHAR(255),
    id_sexo INT,
    id_situacion_laboral INT,
    cantidad INT
);
"""
task_create_fact_situacion_laboral_egresados = MySqlOperator(
    task_id="task_create_fact_situacion_laboral_egresados",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_fact_situacion_laboral_egresados,
    dag=dag,
)

# -----------------------------------------------------------------------------

query_drop_fact_egresados_rama_ensenanza = """
DROP TABLE IF EXISTS fact_egresados_rama_enseñanza;
"""
task_drop_fact_egresados_rama_ensenanza = MySqlOperator(
    task_id="task_drop_fact_egresados_rama_ensenanza",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_fact_egresados_rama_ensenanza,
    dag=dag,
)

query_create_fact_egresados_rama_ensenanza = """
CREATE TABLE IF NOT EXISTS fact_egresados_rama_enseñanza (
	año INT NOT NULL,
	id_pais INT,
    id_universidad INT,
    id_rama_enseñanza INT,
    num_egresados INT
);
"""
task_create_fact_egresados_rama_ensenanza = MySqlOperator(
    task_id="task_create_fact_egresados_rama_ensenanza",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_fact_egresados_rama_ensenanza,
    dag=dag,
)


# -----------------------------------------------------------------------------

query_drop_fact_egresados_niveles = """
DROP TABLE IF EXISTS fact_egresados_niveles;
"""
task_drop_fact_egresados_niveles = MySqlOperator(
    task_id="task_drop_fact_egresados_niveles",
    mysql_conn_id='conexion_mysql', 
    sql=query_drop_fact_egresados_niveles,
    dag=dag,
)

query_create_fact_egresados_niveles = """
CREATE TABLE IF NOT EXISTS fact_egresados_niveles (
	año INT NOT NULL,
	id_pais INT,
    id_ambito VARCHAR(255),
    id_sexo INT,
    id_rango_edad INT,
    num_egresados_nivel_1 INT,
    num_egresados_nivel_2 INT,
    num_egresados INT
);
"""
task_create_fact_egresados_niveles = MySqlOperator(
    task_id="task_create_fact_egresados_niveles",
    mysql_conn_id='conexion_mysql', 
    sql=query_create_fact_egresados_niveles,
    dag=dag,
)


# -----------------------------------------------------------------------------



# definiendo dependiencias

task_start >> task_drop_stage_numero_egresados_internacional >> task_create_stage_numero_egresados_internacional
task_start >> task_drop_stage_porcentaje_egresados_internacional >> task_create_stage_porcentaje_egresados_internacional
task_start >> task_drop_stage_situacion_laboral_egresados >> task_create_stage_situacion_laboral_egresados
task_start >> task_drop_stage_egresados_niveles >> task_create_stage_egresados_niveles
task_start >> task_drop_stage_egresados_universidad >> task_create_stage_egresados_universidad
task_start >> task_drop_stage_ramas_conocimiento >> task_create_stage_ramas_conocimiento

task_start >> task_drop_dimm_pais >> task_create_dimm_pais
task_start >> task_drop_dimm_sexo >> task_create_dimm_sexo
task_start >> task_drop_dimm_tipo_universidad >> task_create_dimm_tipo_universidad
task_start >> task_drop_dimm_universidades >> task_create_dimm_universidades
task_start >> task_drop_dimm_rama_ensenanza >> task_create_dimm_rama_ensenanza
task_start >> task_drop_dimm_ambito_ensenanza >> task_create_dimm_ambito_ensenanza
task_start >> task_drop_dimm_situacion_laboral >> task_create_dimm_situacion_laboral
task_start >> task_drop_dimm_edad >> task_create_dimm_edad

task_start >> task_drop_fact_international_graduated >> task_create_fact_international_graduated
task_start >> task_drop_fact_situacion_laboral_egresados >> task_create_fact_situacion_laboral_egresados
task_start >> task_drop_fact_egresados_rama_ensenanza >> task_create_fact_egresados_rama_ensenanza
task_start >> task_drop_fact_egresados_niveles >> task_create_fact_egresados_niveles

