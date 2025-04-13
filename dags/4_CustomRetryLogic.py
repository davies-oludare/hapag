# This DAG demonstrates how to implement custom retry behavior using task context.
# The `custom_retry_task` inspects the current attempt number and intentionally fails
# until a certain condition is met (e.g., attempt >= 3). This approach gives full control over retries.

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# A task that fails intentionally until the 3rd attempt
def custom_logic(**kwargs):
    ti = kwargs['ti']
    attempt = ti.try_number
    print(f"Attempt {attempt}")
    if attempt < 3:
        raise Exception("Failing on purpose before attempt 3")
    print("Succeeded")

# A generic logging task
def log():
    print("This task is running normally.")

with DAG(
    dag_id="custom_retry_logic",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # Begin the pipeline
    t1 = PythonOperator(task_id="init", python_callable=log)

    # Regular processing step
    t2 = PythonOperator(task_id="step_a", python_callable=log)

    # This task implements custom retry logic using Airflow context
    t3 = PythonOperator(
        task_id="custom_retry_task",
        python_callable=custom_logic,
        retries=5,
        provide_context=True  # allows access to try_number
    )

    # Final processing steps
    t4 = PythonOperator(task_id="step_b", python_callable=log)
    t5 = PythonOperator(task_id="finish", python_callable=log)

    # Define task order
    t1 >> t2 >> t3 >> t4 >> t5