import os
import json
import requests
import pandas as pd # Нам понадобится pandas
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

# Настройки
PROJECT_ID = 'my-learning-de-project'
DATASET_ID = 'raw_data'
TABLE_ID = 'weather_log'
TG_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TG_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

CITIES = {
    "Moscow": {"lat": 55.75, "lon": 37.61},
    "Ufa": {"lat": 54.74, "lon": 55.97},
    "Ekaterinburg": {"lat": 56.84, "lon": 60.61},
    "Lipetsk": {"lat": 52.60, "lon": 39.57},
    "Kazan": {"lat": 55.79, "lon": 49.12}
}

def send_telegram_msg(text):
    if not TG_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TG_CHAT_ID, "text": text})
    except:
        print("Ошибка отправки в ТГ")

def run_pipeline():
    try:
        # 1. Авторизация
        key_json = os.environ.get('BIGQUERY_SERVICE_ACCOUNT_KEY')
        info = json.loads(key_json)
        creds = service_account.Credentials.from_service_account_info(info)
        client = bigquery.Client(project=PROJECT_ID, credentials=creds)

        all_data = []
        for city, coords in CITIES.items():
            print(f"Забираю данные для {city}...")
            url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true"
            res = requests.get(url).json()['current_weather']
            
            all_data.append({
                "city": city,
                "temperature": float(res['temperature']),
                "weather_code": int(res['weathercode']),
                "timestamp": pd.to_datetime(datetime.utcnow()) # Используем формат pandas
            })
            if city == "Ufa":
                send_telegram_msg(f"Уфа: {res['temperature']}°C. Данные собраны!")

        # 2. ЗАГРУЗКА ЧЕРЕЗ LOAD JOB (Бесплатно для Free Tier)
        df = pd.DataFrame(all_data)
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Настройка загрузки
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND", # Добавлять в конец
        )

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result() # Ждем завершения
        
        print(f"Успех! Загружено строк: {len(df)}")

    except Exception as e:
        error_msg = f"⚠️ Ошибка пайплайна: {str(e)}"
        print(error_msg)
        send_telegram_msg(error_msg)
        exit(1)

if __name__ == "__main__":
    run_pipeline()
