from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'crypto_api_dag',
    default_args=default_args,
    description='DAG to fetch and store cryptocurrency exchange rates',
    schedule_interval=timedelta(days=1),  # Ajusta la frecuencia según tus necesidades
)

# Comando bash que ejecutará tu script de Python
bash_command = 'EntregaCoder.py'

# Crea la tarea utilizando el operador BashOperator
run_etl_task = BashOperator(
    task_id='run_etl',
    bash_command=bash_command,
    dag=dag,
)

run_etl_task
