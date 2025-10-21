# ======================================================
# WEATHERFLOW: END-TO-END ETL PIPELINE
# Author: S. Nikitha Mandla
# Description:
#   Extracts real-time weather data for multiple U.S. cities 
#   from the OpenWeatherMap API, transforms it using Python (Pandas + PyJanitor),
#   and loads it into PostgreSQL for analysis.
# ======================================================
# PostgreSQL Table Schema
# ------------------------------------------------------
# Run this SQL command in pgAdmin before executing the Python script
# to create the table used for inserting raw weather data.
#
# CREATE TABLE weather_db (
#     id SERIAL PRIMARY KEY,
#     city TEXT,
#     weather TEXT,
#     temperature FLOAT,
#     humidity INTEGER,
#     wind_speed FLOAT,
#     recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# );

# ------------------- IMPORTS -------------------
import requests
import psycopg2
import time
import pandas as pd
from sqlalchemy import create_engine
import janitor
import os
# ------------------------------------------------


# ------------------- API CONFIG -----------------
API_KEY = '011e7fea2e93c147086933334b88c8ab'

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
    "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington"
]

API_URL_TEMPLATE = "http://api.openweathermap.org/data/2.5/weather?q={city}&appid={key}&units=metric"
"""
API Reference:
  - Base URL: /data/2.5/weather
  - Parameters:
        q       → City name
        appid   → API key
        units   → Metric (Celsius)
"""
# ------------------------------------------------


# ------------------- DATABASE CONFIG -----------------
db_config = {
    "host": "localhost",
    "database": "weather_db",
    "user": "postgres",
    "password": "Nikki@1435",
    "port": "5432"
}

conn = psycopg2.connect(**db_config)
cursor = conn.cursor()
# ------------------------------------------------


# ------------------- RAW TABLE INSERT QUERY -----------------
insert_query = """
INSERT INTO weather_db(city, weather, temperature, humidity, wind_speed)
VALUES (%s, %s, %s, %s, %s);
"""
# ------------------------------------------------


# ------------------- EXTRACT + RAW LOAD -----------------
for city in CITIES:
    api_url = API_URL_TEMPLATE.format(city=city, key=API_KEY)
    response = requests.get(api_url)

    if response.status_code == 200:
        weather_json = response.json()
        weather_desc = weather_json['weather'][0]['description']
        temperature = weather_json['main']['temp']
        humidity = weather_json['main']['humidity']
        wind_speed = weather_json['wind']['speed']

        cursor.execute(insert_query, (city, weather_desc, temperature, humidity, wind_speed))
        print(f"{city}: data inserted successfully")
    else:
        print(f"{city}: failed to fetch data (status {response.status_code})")

    time.sleep(1)

conn.commit()
cursor.close()
conn.close()
print("Weather data for all cities inserted into PostgreSQL successfully.")
# ------------------------------------------------


# ------------------- TRANSFORM PHASE -----------------
engine = create_engine('postgresql://postgres:Nikki%401435@localhost:5432/weather_db')

conn = engine.connect()
print("Connected to PostgreSQL successfully.")
conn.close()

df = pd.read_sql('SELECT * FROM weather_db;', engine)

# Cleaning & Transformation
humidity_mean = df['humidity'].mean()

df_cleaned = (
    df
    .clean_names() 
    .remove_empty()
    .fill_empty(columns=['city'], value='UNKNOWN')
)

df_cleaned['humidity'] = df_cleaned['humidity'].fillna(humidity_mean)
df_cleaned['temperature'] = df_cleaned['temperature'].round(1)
df_cleaned['city'] = df_cleaned['city'].str.upper()

def categorize_temp(temp):
    if pd.isna(temp):
        return 'Unknown'
    elif temp >= 35:
        return 'High'
    elif 25 <= temp < 35:
        return 'Moderate'
    else:
        return 'Low'

df_cleaned['temp_category'] = df_cleaned['temperature'].apply(categorize_temp)

df_cleaned.to_sql('USA_weather_transformed', engine, if_exists='replace', index=False)
print("Transformed data successfully loaded into PostgreSQL table: USA_weather_transformed")
# ------------------------------------------------


# ------------------- DATA CONSUMER PHASE -----------------
db_config = {
    "host": "localhost",
    "database": "weather_db",
    "user": "postgres",
    "password": "Nikki%401435",
    "port": "5432"
}

engine = create_engine(
    f"postgresql://{db_config['user']}:{db_config['password']}@"
    f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

query = 'SELECT * FROM "USA_weather_transformed";'
weather_df = pd.read_sql(query, engine)
print("Preview of transformed data:")
print(weather_df.head())

weather_df.to_csv("USA_weather_data.csv", index=False)
print("Data successfully saved to 'USA_weather_data.csv'.")

print("Current directory:", os.getcwd())
# ------------------------------------------------

# END OF SCRIPT


