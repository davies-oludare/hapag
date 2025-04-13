# This DAG demonstrates how to use exponential backoff with retries.
# The `unstable_task` simulates a flaky task that fails multiple times,
# and retries with increasing delays between attempts, helping to reduce strain on external systems.

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Simulates a flaky/unreliable task
def unreliable():
    raise Exception("Still broken!")

# Simulates a reliable step
def stable_step():
    print("This step works fine.")

# Logs pipeline start or end
def log_start():
    print("Start of the pipeline.")

with DAG(
    dag_id="retry_with_exponential_backoff",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # Log start of the DAG
    t1 = PythonOperator(task_id="start", python_callable=log_start)

    # A normal task
    t2 = PythonOperator(task_id="step1", python_callable=stable_step)

    # Failing task with exponential backoff (retry delay doubles on each failure)
    t3 = PythonOperator(
        task_id="unstable_task",
        python_callable=unreliable,
        retries=4,
        retry_delay=timedelta(seconds=15),
        retry_exponential_backoff=True, #Exponential backoff will double the wait time between retries after every retry
        max_retry_delay=timedelta(minutes=5)
    )

    # Another normal task
    t4 = PythonOperator(task_id="step2", python_callable=stable_step)

    # Log the end of the pipeline
    t5 = PythonOperator(task_id="end", python_callable=log_start)

    # Set task order
    t1 >> t2 >> t3 >> t4 >> t5