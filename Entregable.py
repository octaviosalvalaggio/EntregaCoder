import requests
import json
from sqlalchemy import create_engine
import pandas as pd


# Función para obtener tasas de cambio de criptomonedas atraves de la API
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

# Se verifica si las tasas de cambio se obtuvierón con exito
if tasas_de_cambio is not None:
    print("Tasas de cambio para las criptomonedas:")
    print(tasas_de_cambio)
    
    # Guardar los datos en un archivo JSON
    with open('tasas_de_cambio.json', 'w') as archivo_json:
        json.dump(tasas_de_cambio, archivo_json, indent=4)
        print("Datos guardados en 'tasas_de_cambio.json'")

#Credenciales de Coderhouse
DB_USERNAME= 'octaviosalvalaggio_coderhouse'
DB_PASSWORD= 'egUB42f08N'
DB_HOST= 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
DB_PORT= '5439'
DB_NAME= 'data-engineer-database'

# URL de conexión
url = f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Conexión a la base de datos
conn = create_engine(url)

# Verificar si las tasas de cambio se obtuvieron con éxito antes de continuar
if tasas_de_cambio is not None:
    # Convertir los datos en un DataFrame de Pandas
    df = pd.DataFrame(tasas_de_cambio)
    
    # Renombrar las columnas del DataFrame para que coincidan con la tabla en dbeaver
    df = df.rename(columns={'moneda': 'moneda', 'date': 'date', 'tasa_cambio': 'tasa_cambio'})
    
    # Insertar los datos en la tabla 'tasas_de_cambio'
    df.to_sql('tasas_de_cambio', conn, if_exists='replace', index=False)
    
    print("Datos insertados en la tabla 'tasas_de_cambio'")
