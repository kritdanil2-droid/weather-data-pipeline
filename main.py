import os
import json
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

# Настройки проекта
PROJECT_ID = 'my-learning-de-project'
DATASET_ID = 'raw_data'
TABLE_ID = 'weather_log'

# Настройки Телеграм 
TG_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TG_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

# Список городов и их координаты
CITIES = {
    "Moscow": {"lat": 55.75, "lon": 37.61},
    "Ufa": {"lat": 54.74, "lon": 55.97},
    "Ekaterinburg": {"lat": 56.84, "lon": 60.61},
    "Lipetsk": {"lat": 52.60, "lon": 39.57},
    "Kazan": {"lat": 55.79, "lon": 49.12}
}

def send_telegram_msg(text):
    if not TG_CHAT_ID:
        print("Telegram Chat ID не настроен, пропуск уведомления.")
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text}
    requests.post(url, json=payload)

def run_pipeline():
    # 1. Авторизация в BigQuery
    key_json = os.environ.get('BIGQUERY_SERVICE_ACCOUNT_KEY')
    if not key_json:
        print("Ошибка: Секрет BIGQUERY_SERVICE_ACCOUNT_KEY не найден!")
        return

    info = json.loads(key_json)
    creds = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    all_rows = []
    
    # 2. Цикл по городам
    for city, coords in CITIES.items():
        print(f"Забираю данные для {city}...")
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true"
        
        try:
            response = requests.get(url)
            data = response.json()['current_weather']
            temp = data['temperature']
            
            all_rows.append({
                "city": city,
                "temperature": temp,
                "weather_code": int(data['weathercode']),
                "timestamp": datetime.utcnow().isoformat()
            })
            
            # 3. Логика уведомления для Уфы
            if city == "Ufa":
                # Условие: если температура ниже 0 (или просто для теста при каждом запуске)
                # Давай для теста сделаем уведомление всегда, чтобы ты проверил работу:
                status = "морозно" if temp < 0 else "тепло"
                msg = f"Привет из Уфы! 🏔\nТекущая температура: {temp}°C. На улице {status}."
                send_telegram_msg(msg)
                
        except Exception as e:
            print(f"Ошибка при обработке {city}: {e}")

    # 4. Массовая загрузка в BigQuery
    if all_rows:
        errors = client.insert_rows_json(table_id, all_rows)
        if errors == []:
            print(f"Успех! Добавлено строк: {len(all_rows)}")
        else:
            print(f"Ошибки при вставке: {errors}")

if __name__ == "__main__":
    run_pipeline()
