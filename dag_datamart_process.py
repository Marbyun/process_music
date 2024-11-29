from datetime import timedelta, datetime
import logging
import pendulum
import pandas as pd
from sqlalchemy import create_engine
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.dummy_operator import DummyOperator

import queries.etl_datamart_process as etl

conn_postgre = 'local_postgresql'

log = logging.getLogger(__name__)

local_tz = pendulum.timezone("Asia/Jakarta")
date_today = datetime.now()

with DAG(
        dag_id='dag_datamart_process',
        start_date=pendulum.datetime(2024, 11, 25, tz='Asia/Jakarta'),
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
) as dag:
    start_task = DummyOperator(
        task_id = 'Start',
    )

    end_task = DummyOperator(
        task_id = 'End',
    )

    del_data = SQLExecuteQueryOperator(
        task_id = 'del_datamart',
        conn_id = conn_postgre,
        sql = etl.DELETE_DATA
    )

    ins_data = SQLExecuteQueryOperator(
        task_id = 'ins_datamart',
        conn_id = conn_postgre,
        sql = etl.INSERT_DATA
    )

    start_task >> del_data >> ins_data >> end_task

