import requests
import json
from sqlalchemy import create_engine
import pandas as pd
from decouple import config
from sqlalchemy.exc import IntegrityError  # Importar esta excepción

# Función para obtener tasas de cambio de criptomonedas a través de la API
def obtener_tasas_de_cambio(criptos):
    url = 'https://api.exchangerate.host/latest'
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

# Se verifica si las tasas de cambio se obtuvieron con éxito
if tasas_de_cambio is not None:
    print("Tasas de cambio para las criptomonedas:")
    print(tasas_de_cambio)
    
    # Guardar los datos en un archivo JSON
    with open('tasas_de_cambio.json', 'w') as archivo_json:
        json.dump(tasas_de_cambio, archivo_json, indent=4)
        print("Datos guardados en 'tasas_de_cambio.json'")

# Credenciales desde el archivo .env
username = config('DB_USERNAME')
password = config('DB_PASSWORD')
host = config('DB_HOST')
port = config('DB_PORT')
database_name = config('DB_NAME')

# URL de conexión
url = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'

# Conexión a la base de datos
conn = create_engine(url)

# Verificar si las tasas de cambio se obtuvieron con éxito antes de continuar
if tasas_de_cambio is not None:
    # Convertir los datos en un DataFrame de Pandas
    df = pd.DataFrame(tasas_de_cambio)
    
    # Renombrar las columnas del DataFrame para que coincidan con la tabla en DBeaver
    df = df.rename(columns={'base': 'moneda_base', 'date': 'fecha', 'rates': 'tasas'})
    
    try:
        # Insertar los datos en la tabla 'tasas_de_cambio', evitando duplicados
        df.to_sql('tasas_de_cambio', conn, if_exists='append', index=False)
        print("Datos insertados en la tabla 'tasas_de_cambio'")
    except IntegrityError as e:
        print("Error al insertar datos en la tabla 'tasas_de_cambio':", e)
