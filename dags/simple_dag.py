# ./dags/simple_dag.py
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='my_first_docker_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['playground'],
) as dag:
    # Task 1: Print the current date
    print_date = BashOperator(
        task_id='print_the_date',
        bash_command='echo "Today is $(date)"',
    )

    # Task 2: Sleep for 5 seconds
    sleep_task = BashOperator(
        task_id='sleep_for_a_bit',
        bash_command='sleep 5',
    )

    # Task 3: Echo a success message
    echo_message = BashOperator(
        task_id='final_message',
        bash_command='echo "Simple DAG completed successfully!"',
    )

    # Define the dependencies (workflow)
    print_date >> sleep_task >> echo_message