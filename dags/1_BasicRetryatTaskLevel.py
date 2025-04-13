# This DAG demonstrates how to configure retries on a task level.
# The task `failing_task` will fail intentionally and retry 3 times before failing permanently.
# This is a foundational practice to handle transient failures like flaky network or API calls.

# What You’ll See in the UI:
#	•	failing_task will show 4 total attempts (1 initial + 3 retries).
#	•	Each attempt will show a log line like:

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Always fails to show retries + failure
def fail_task_with_context(**context):
    try_number = context['ti'].try_number
    max_tries = context['ti'].max_tries
    print(f"Task attempt {try_number} of {max_tries + 1}")
    raise ValueError("Intentional failure to trigger retries")

# Simple log tasks
def start():
    print("Starting pipeline")

def process():
    print("Processing data")

def end():
    print("Pipeline complete")

with DAG(
    dag_id="retry_demo_task_level",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="start", python_callable=start)
    t2 = PythonOperator(task_id="process_1", python_callable=process)

    # Always fails, retry 3 times with 10s delay
    t3 = PythonOperator(
        task_id="failing_task",
        python_callable=fail_task_with_context,
        retries=3,
        retry_delay=timedelta(seconds=10),
        provide_context=True,  # Required to access `**context`
    )

    t4 = PythonOperator(task_id="process_2", python_callable=process)
    t5 = PythonOperator(task_id="end", python_callable=end)

    t1 >> t2 >> t3 >> t4 >> t5