import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

# Настройки
PROJECT_ID = 'my-learning-de-project'
DATASET_ID = 'raw_data'
TABLE_ID = 'weather_log'

def run_pipeline():
    # 1. Получаем данные (Open-Meteo - Москва)
    url = url = "https://api.open-meteo.com/v1/forecast?latitude=55.75&longitude=37.61&current_weather=true"
    response = requests.get(url)
    data = response.json()['current_weather']
    
    # 2. Формируем строку
    row_to_insert = [{
        "city": "Moscow",
        "temperature": data['temperature'],
        "weather_code": int(data['weathercode']),
        "timestamp": datetime.utcnow().isoformat()
    }]
    
    # 3. Авторизация в BigQuery
    key_json = os.environ.get('BIGQUERY_SERVICE_ACCOUNT_KEY')
    if not key_json:
        print("Ошибка: Секрет BIGQUERY_SERVICE_ACCOUNT_KEY не найден!")
        return

    info = json.loads(key_json)
    creds = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    
    # 4. Загрузка
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    errors = client.insert_rows_json(table_id, row_to_insert)
    
    if errors == []:
        print(f"Успех! Данные добавлены в {table_id}")
    else:
        print(f"Ошибки при вставке: {errors}")

if __name__ == "__main__":
    run_pipeline()
