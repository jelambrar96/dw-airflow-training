import csv
import logging

import pandas as pd

from airflow.hooks.mysql_hook import MySqlHook


# https://gist.github.com/imamdigmi/f11a9a9e1ab88d2d9e0c3ad626c4ec2d
def func_bulk_load_sql_2(csv_file, table_name, mysql_conn_id):
    local_filepath = csv_file
    """
    conn = MySqlHook(mysql_conn_id='conexion_mysql_root')
    conn.bulk_load(table_name, local_filepath)
    return table_name
    """
    # https://stackoverflow.com/questions/70966865/error-loading-delimited-file-into-mysql-using-airflow-error-code-2068
    mysqlHook = MySqlHook(mysql_conn_id=mysql_conn_id)
    #cursor = conn.cursor()
    conn = mysqlHook.get_conn()
    cursor = conn.cursor()
    csv_data = csv.reader(open(local_filepath))
    header = next(csv_data)
    headers_str = ", ".join(header)
    for row in csv_data:
        new_row_list = ["'{}'".format(item) for item in row]
        new_row_str = ", ".join(new_row_list)
        # logging.info(new_row_str)
        # logging.info(new_row_list)
        q = "INSERT INTO {} ({}) VALUES ({})".format(table_name, headers_str, new_row_str)
        # logging.info(q)
        cursor.execute(q)
    conn.commit()
    cursor.close()


def func_bulk_load_sql(csv_file, table_name, mysql_conn_id):
    # local_filepath = csv_file
    df = pd.read_csv(csv_file)
    mysqlHook = MySqlHook(mysql_conn_id=mysql_conn_id)
    mysqlHook.insert_rows(table=table_name, rows=df.values.tolist(), target_fields=list(df.columns))


def func_load_dataframe(df, table_name, mysql_conn_id):
    mysqlHook = MySqlHook(mysql_conn_id=mysql_conn_id)
    mysqlHook.insert_rows(table=table_name, rows=df.values.tolist(), target_fields=list(df.columns))


def func_download_dataframe(fields, table, mysql_conn_id):
    fields_str = ", ".join(fields)
    query = "SELECT DISTINCT {} FROM {}".format(fields_str, table)
    mysqlHook = MySqlHook(mysql_conn_id=mysql_conn_id)
    results = mysqlHook.get_records(sql=query)
    df = pd.DataFrame(results, columns=fields) if results else pd.DataFrame(columns=fields)
    return df
