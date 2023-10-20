import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
from sqlalchemy import create_engine
import pandas as pd
from decouple import config
from sqlalchemy.exc import IntegrityError
from airflow.operators.email import EmailOperator


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
)

# Función para obtener tasas de cambio de criptomonedas a través de la API
def obtener_tasas_de_cambio(criptos):
    url = 'http://api.exchangerate.host/live?access_key=aff6cf9aaecb91816550ecc09abb624b'
    params = {'symbols': ','.join(criptos)}
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error al hacer la solicitud a la API: {e}")
        return None

# Lista de símbolos de criptomonedas (10 Categorías)
symbols = ['BGN', 'BBD', 'EUR', 'BTN', 'AZN', 'KZT', 'KES', 'LYD', 'MDL', 'AMD']

# Se llama a la función para obtener las tasas de cambio
tasas_de_cambio = obtener_tasas_de_cambio(symbols)

# Verificar si las tasas de cambio se obtuvieron con éxito antes de continuar
if tasas_de_cambio is not None:
    if "motd" in tasas_de_cambio:
        del tasas_de_cambio["motd"]


    # Convertir los datos en un DataFrame de Pandas
    df = pd.DataFrame(tasas_de_cambio)


    def print_dataframe():
        print("DataFrame antes de transformaciones:")
    print(df)
    
    print_dataframe_task = PythonOperator(
        task_id='print_dataframe',
        python_callable=print_dataframe,
        dag=dag,
    )
    
    # Credenciales desde el archivo .env
    username = config('DB_USERNAME')
    password = config('DB_PASSWORD')
    host = config('DB_HOST')
    port = config('DB_PORT')
    database_name = config('DB_NAME')

    # URL de conexión
    url = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'

    conn = create_engine(url)

    def execute_etl():
        print("Ejecutando la lógica de ETL")


    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df['rate'] = df['rate'].astype(float)
        try:
            df.to_sql('tasas_de_cambio', conn, if_exists='replace', index=False)
            print("Datos insertados en la tabla 'tasas_de_cambio'")
        except IntegrityError as e:
            print("Error al insertar datos en la tabla 'tasas_de_cambio':", e)
    else:
        print("La columna 'date' no está presente en el DataFrame.")

    def finish_execution():
        print("¡Ejecución del DAG completada con éxito!")
        return True  # Agrega un retorno explícito
    
    
# Tareas en el DAG
run_etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=execute_etl,
    dag=dag,
)

finish_execution_task = PythonOperator(
    task_id='finish_execution',
    python_callable=finish_execution,
    dag=dag,
)

email_task = EmailOperator(
    task_id='send_email',
    email_on_failure= True,
    to='datavisual.bi@gmail.com',
    subject='Ejecución del DAG completada con éxito',
    html_content='¡La ejecución del DAG ha finalizado con éxito!',
    dag=dag,
)

print_dataframe_task >> run_etl_task
run_etl_task >> finish_execution_task
finish_execution_task >> email_task  