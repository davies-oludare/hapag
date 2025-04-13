# This DAG shows how to design a pipeline that is idempotent—safe to rerun multiple times.
# Each task just logs output, doing no external mutations. This is best practice for retryable pipelines
# because rerunning them doesn't introduce side effects or duplicate results.

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# A repeatable, side-effect-free task
def step():
    print("This step is safe to run multiple times.")

with DAG(
    dag_id="idempotent_task_demo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # Each task is functionally the same—logging and retryable
    t1 = PythonOperator(task_id="step1", python_callable=step, retries=2)
    t2 = PythonOperator(task_id="step2", python_callable=step, retries=2)
    t3 = PythonOperator(task_id="step3", python_callable=step, retries=2)
    t4 = PythonOperator(task_id="step4", python_callable=step, retries=2)
    t5 = PythonOperator(task_id="step5", python_callable=step, retries=2)

    # Tasks run in order
    t1 >> t2 >> t3 >> t4 >> t5