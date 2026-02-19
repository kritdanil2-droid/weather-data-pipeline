import os
import json
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

# --- Configuration --- #
# Replace with your actual BigQuery details
BIGQUERY_PROJECT_ID = 'your-gcp-project-id'  # e.g., 'my-gcp-project-12345'
BIGQUERY_DATASET_ID = 'weather_data_raw'      # e.g., 'weather_dataset'
BIGQUERY_TABLE_ID = 'current_weather'         # e.g., 'current_conditions'

# Open-Meteo API details for Moscow
WEATHER_API_URL = 'https://api.open-meteo.com/v1/forecast'
WEATHER_LATITUDE = 55.7522
WEATHER_LONGITUDE = 37.6156
WEATHER_CITY_NAME = 'Moscow' # For transformation

def get_bigquery_client():
    """Initializes and returns a BigQuery client."""
    try:
        # Retrieve BigQuery service account key from environment variable
        bigquery_key_json = os.getenv('BIGQUERY_KEY_JSON')
        if not bigquery_key_json:
            raise ValueError("BIGQUERY_KEY_JSON environment variable not set.")

        credentials_info = json.loads(bigquery_key_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)

        client = bigquery.Client(project=BIGQUERY_PROJECT_ID, credentials=credentials)
        print("BigQuery client initialized successfully.")
        return client
    except Exception as e:
        print(f"Error initializing BigQuery client: {e}")
        return None

def fetch_weather_data(api_url, latitude, longitude, city_name):
    """Fetches weather data from Open-Meteo API for specified coordinates."""
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'current_weather': 'true',
        'timezone': 'Europe/Moscow' # Or 'auto'
    }

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        weather_data = response.json()
        print(f"Successfully fetched weather data for {city_name} (Lat: {latitude}, Lon: {longitude}).")
        return weather_data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - Response: {response.text}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An unexpected error occurred during the request: {req_err}")
    except json.JSONDecodeError as json_err:
        print(f"Error decoding JSON response: {json_err} - Response: {response.text}")
    return None

def transform_weather_data(raw_data, city_name):
    """Transforms raw Open-Meteo weather data into BigQuery-compatible format (city, temperature, weather_code, timestamp)."""
    if not raw_data or 'current_weather' not in raw_data:
        print("Invalid raw data for transformation or 'current_weather' key missing.")
        return []

    try:
        current_weather = raw_data['current_weather']
        transformed = {
            'city': city_name,
            'temperature': current_weather.get('temperature'),
            'weather_code': current_weather.get('weathercode'),
            'timestamp': current_weather.get('time') # ISO 8601 string, BigQuery TIMESTAMP can parse this
        }
        return [transformed]
    except Exception as e:
        print(f"Error transforming weather data: {e}")
        return []

def insert_data_to_bigquery(client, dataset_id, table_id, data):
    """Inserts data into a BigQuery table."""
    if not client or not data:
        print("No data to insert or BigQuery client not initialized.")
        return

    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        # BigQuery expects a list of dictionaries for insert_rows_json
        errors = client.insert_rows_json(table_ref, data)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")
        else:
            print(f"Successfully inserted {len(data)} rows into {dataset_id}.{table_id}")
    except Exception as e:
        print(f"Error during BigQuery data insertion: {e}")

if __name__ == "__main__":
    # 1. Get BigQuery Client
    bq_client = get_bigquery_client()
    if not bq_client:
        print("Exiting due to BigQuery client initialization failure.")
        exit(1)

    # 2. Fetch Weather Data
    weather_raw_data = fetch_weather_data(WEATHER_API_URL, WEATHER_LATITUDE, WEATHER_LONGITUDE, WEATHER_CITY_NAME)

    if not weather_raw_data:
        print("Exiting due to weather data fetch failure.")
        exit(1)

    # 3. Transform Data
    weather_transformed_data = transform_weather_data(weather_raw_data, WEATHER_CITY_NAME)
    if not weather_transformed_data:
        print("Exiting due to weather data transformation failure.")
        exit(1)

    # 4. Insert Data into BigQuery
    insert_data_to_bigquery(bq_client, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID, weather_transformed_data)
