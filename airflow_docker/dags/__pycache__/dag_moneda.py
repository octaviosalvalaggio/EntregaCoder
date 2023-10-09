from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

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
    schedule_interval=timedelta(days=1),
)

# Comando bash que ejecutará tu script de Python
python_command = 'Entregable.py'

# Tarea para ejecutar el script ETL
def execute_etl_function():
    # Coloca aquí las operaciones ETL que deseas realizar
    pass

run_etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=execute_etl_function,
    dag=dag,
)

# Tarea para limpiar después de la ejecución del script ETL
def clean_up_function():
    # Coloca aquí las operaciones de limpieza que deseas realizar
    pass

clean_up_task = PythonOperator(
    task_id='clean_up',
    python_callable=clean_up_function,
    dag=dag,
)

# Define el orden de ejecución de las tareas
run_etl_task >> clean_up_task

run_etl_task
