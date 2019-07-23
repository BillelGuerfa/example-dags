from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator



default_args = {
    'owner': 'airflow',
    'retries': 0,
    'start_date': airflow.utils.dates.days_ago(2)
    }


dag = DAG(dag_id='internship_pipeline_bash', default_args=default_args, schedule_interval='0 0 * * *')

run_this_first = DummyOperator(
    task_id='run_this_last',
    dag=dag,
)

clean_so_data_bash = BashOperator(
    task_id='clean_so_data_bash',
    bash_command='spark-submit --master k8s://https://35.230.26.119 --deploy-mode cluster --name spark-test --conf spark.kubernetes.namespace=default --conf spark.executor.instances=2 --class "Main" --conf spark.kubernetes.container.image=gcr.io/billel-internship/spark --conf spark.kubernetes.container.image.pullPolicy=Always --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark local:///jobs/StackOverflowPreprocessor-assembly-0.1.0-SNAPSHOT.jar',
    dag=dag,
)


run_this_first >> clean_so_data_bash 