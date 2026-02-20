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
TG_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TG_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

CITIES = {
    "Moscow": {"lat": 55.75, "lon": 37.61},
    "Ufa": {"lat": 54.74, "lon": 55.97},
    "Ekaterinburg": {"lat": 56.84, "lon": 60.61},
    "Lipetsk": {"lat": 52.60, "lon": 39.57},
    "Kazan": {"lat": 55.79, "lon": 49.12}
}

def get_weather_description(code):
    """Расшифровка кодов погоды WMO (World Meteorological Organization)"""
    weather_codes = {
        0: "Ясно ☀️",
        1: "В основном ясно 🌤",
        2: "Переменная облачность ⛅",
        3: "Пасмурно ☁️",
        45: "Туман 🌫",
        48: "Иней ❄️",
        51: "Слабая морось 🌧",
        53: "Умеренная морось 🌧",
        55: "Сильная морось 🌧",
        56: "Слабая ледяная морось ❄️🌧",
        57: "Сильная ледяная морось ❄️🌧",
        61: "Небольшой дождь 🌦",
        63: "Дождь 🌧",
        65: "Сильный дождь ⛈",
        66: "Слабый ледяной дождь ❄️🌧",
        67: "Сильный ледяной дождь ❄️🌧",
        71: "Небольшой снег 🌨",
        73: "Снег ❄️",
        75: "Сильный снег ❄️❄️",
        77: "Снежные зерна ❄️",
        80: "Слабый ливень 🌧",
        81: "Умеренный ливень 🌧",
        82: "Сильный ливень ⛈",
        85: "Небольшой снегопад 🌨",
        86: "Сильный снегопад ❄️❄️",
        95: "Гроза 🌩",
        96: "Гроза со слабым градом ⛈",
        99: "Гроза с сильным градом ⛈"
    }
    return weather_codes.get(code, "Неизвестно ❓")

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
            
            temp = float(res['temperature'])
            wind_speed = float(res['windspeed'])
            w_code = int(res['weathercode'])
            w_desc = get_weather_description(w_code)
            
            all_data.append({
                "city": city,
                "temperature": temp,
                "wind_speed": wind_speed,
                "weather_code": w_code,
                "weather_desc": w_desc,
                "timestamp": pd.to_datetime(datetime.utcnow())
            })
            
            # Красивое уведомление для Уфы
            if city == "Ufa":
                msg = (
                    f"🏙 Уфа:\n"
                    f"🌡 Температура: {temp}°C\n"
                    f"💨 Скорость ветра: {wind_speed} км/ч\n"
                    f"☁️ За окном: {w_desc}\n"
                    f"✅ Данные успешно собраны!"
                )
                send_telegram_msg(msg)

        # 2. ЗАГРУЗКА В BIGQUERY
        df = pd.DataFrame(all_data)
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Настройка загрузки
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            # ВАЖНО: Разрешаем добавление новых колонок в существующую таблицу
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
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
