from __future__ import print_function
import airflow
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import os

JOBS_PATH = "/home/jobs/"


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'retries': 0,
    'start_date': airflow.utils.dates.days_ago(2)
    }


dag = DAG(dag_id='internship_pipeline', default_args=default_args, schedule_interval=timedelta(days=1))


clean_so_data = SparkSubmitOperator(
    task_id='clean_so_data',
    application="local:///jobs/StackOverflowPreprocessor-assembly-0.1.0-SNAPSHOT.jar",
    name = "clean_so_data",
    num_executors = 3,
    conn_id="spark_default",
    java_class = "Main",
    conf={
        "spark.kubernetes.namespace" : "default",
        "spark.kubernetes.container.image":"gcr.io/billel-internship/spark",
        "spark.kubernetes.container.image.pullPolicy":"Always",
        "spark.hadoop.google.cloud.auth.service.account.enable": "true",
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile": "/mnt/secrets/spark-sa.json"
    },
    dag=dag
)

clean_so_data