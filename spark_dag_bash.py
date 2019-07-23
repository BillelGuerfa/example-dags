import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'start_date': airflow.utils.dates.days_ago(2)
    }


dag = DAG(dag_id='internship_pipeline_bash', default_args=default_args, schedule_interval=timedelta(days=1))


clean_so_data_bash = BashOperator(
    task_id='clean_so_data_bash',
    bash_command='spark-submit --master k8s://https://35.230.26.119 --deploy-mode cluster --name spark-test --conf spark.kubernetes.namespace=default --conf spark.executor.instances=2 --class "Main" --conf spark.kubernetes.container.image=gcr.io/billel-internship/spark --conf spark.kubernetes.container.image.pullPolicy=Always --conf spark.hadoop.google.cloud.auth.service.account.enable=true --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/mnt/secrets/spark-sa.json --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-admin --conf spark.kubernetes.driverEnv.GCS_PROJECT_ID billel-internship --conf spark.kubernetes.driverEnv.GOOGLE_APPLICATION_CREDENTIALS /mnt/secrets/spark-sa.json --conf spark.kubernetes.driver.secrets.spark-sa  /mnt/secrets --conf spark.kubernetes.executor.secrets.spark-sa /mnt/secrets local:///jobs/StackOverflowPreprocessor-assembly-0.1.0-SNAPSHOT.jar',
    dag=dag,
)

clean_so_data_bash