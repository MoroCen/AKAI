import requests
import sqlite3
import json

DB_FILE = "weather_elt.db"

CITIES = [
    {
        "city": "Manila",
        "latitude": 14.5995,
        "longitude": 120.9842
    },
    {
        "city": "Cebu",
        "latitude": 10.3157,
        "longitude": 123.8854
    }
]


def extract_weather_data(city):
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={city['latitude']}"
        f"&longitude={city['longitude']}"
        "&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code"
        "&timezone=Asia%2FManila"
    )

    response = requests.get(url)
    response.raise_for_status()

    return response.json()


def connect_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    return conn, cursor


def create_raw_table(cursor):
    """
    TODO #1: CREATE TABLE

    Create a table named raw_weather.

    Columns:
    id INTEGER PRIMARY KEY AUTOINCREMENT
    city TEXT
    latitude REAL
    longitude REAL
    raw_json TEXT
    """

    # Write CREATE TABLE code here
    pass


def insert_raw_weather(cursor, city, weather_data):
    """
    TODO #2: INSERT INTO

    Insert raw API weather data into raw_weather.

    Fields:
    city
    latitude
    longitude
    raw_json

    Hint:
    Use json.dumps(weather_data) to convert JSON into text.
    """

    # Write INSERT INTO code here
    pass


def transform_data_inside_database(cursor):
    cursor.execute("DROP TABLE IF EXISTS transformed_weather")

    cursor.execute("""
        CREATE TABLE transformed_weather AS
        SELECT
            id,
            city,
            latitude,
            longitude,
            json_extract(raw_json, '$.current.time') AS observation_time,
            json_extract(raw_json, '$.current.temperature_2m') AS temperature_celsius,
            json_extract(raw_json, '$.current.relative_humidity_2m') AS humidity,
            json_extract(raw_json, '$.current.wind_speed_10m') AS wind_speed,
            json_extract(raw_json, '$.current.weather_code') AS weather_code
        FROM raw_weather
    """)


def select_transformed_weather(cursor):
    """
    TODO #3: SELECT FROM

    Select and display all records from transformed_weather.

    Expected columns:
    id, city, latitude, longitude, observation_time,
    temperature_celsius, humidity, wind_speed, weather_code
    """

    # Write SELECT code here
    pass


def main():
    conn, cursor = connect_database()

    create_raw_table(cursor)

    cursor.execute("DELETE FROM raw_weather")
    conn.commit()

    for city in CITIES:
        weather_data = extract_weather_data(city)
        insert_raw_weather(cursor, city, weather_data)

    transform_data_inside_database(cursor)

    conn.commit()

    select_transformed_weather(cursor)

    conn.close()


if __name__ == "__main__":
    main()