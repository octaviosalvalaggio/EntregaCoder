# ENTREGACODER

#1) ENTREGA N°1:

Se extrae una API Pública 

API: https://exchangerate.host/

Se extraen los datos financieros de 10 criptomonedas:

1• "BGN"
2• "BBD"
3• "EUR"
4• "BTN"
5• "AZN"
6• "KZT"
7• "KES"
8• "LYD"
9• "MDL"
10• "AMD"

Se crea .env para cargar las credenciales

Se crea una conexión a Redshift para la carga de datos.

#2) ENTREGA N°2:
   
Se creo la tabla en Dbeaver con Redshift

Cargar los datos leídos de la API en la tabla.

#3) ENTREGA N°3:

Se creo el container con el nombre "Dockerfile"

Entrega de un Dockerfile para crear una imagen y un contenedor.

Se creo DAG con nombre "dag_moneda.py" para conectar con Apache Airflow utilizando PythonOperator

DAG:
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crypto_api_dag',
    default_args=default_args,
    description='DAG to fetch and store cryptocurrency exchange rates',
    schedule_interval=timedelta(days=1),

Se ejecuta atraves de docker-compose up, las tareas que se ejecutan son:

print_dataframe_task >> run_etl_task
run_etl_task >> finish_execution_task

#4) ENTREGA N°4:

Se incorpora al proyecto el envío de alertas mediante SMTP, atraves de EmailOperator.

Se crea la Tarea email_task

email_task = EmailOperator(
    task_id='send_email',
    email_on_failure= True,
    to='datavisual.bi@gmail.com',
    subject='Ejecución del DAG completada con éxito',
    html_content='¡La ejecución del DAG ha finalizado con éxito!',
    dag=dag,
)

finish_execution_task >> email_task  

Se adjunta imagen del proceso.
