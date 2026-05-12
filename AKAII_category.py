import requests
import sqlite3
import json
import os
import sys

# --- Configuration ---
DB_FILE = "weather_elt.db"

# Cities and their coordinates in the Philippines for the pipeline to process.
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


# ==========================================
# E - Extract Phase
# ==========================================

def extract_weather_data(city):
    """
    Extracts current weather data from the Open-Meteo API.
    
    The API request specifies temperature, humidity, wind speed, and weather code
    for the specific city's coordinates.
    """
    print(f"--- Extracting weather data for {city['city']} ---")
    
    # Define the API endpoint and parameters. Open-Meteo is a free API.
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={city['latitude']}"
        f"&longitude={city['longitude']}"
        "&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code"
        "&timezone=Asia%2FManila"
    )

    try:
        response = requests.get(url)
        # Raise an exception if the request returned an unsuccessful status code (e.g., 404, 500)
        response.raise_for_status()
        
        # Parse and return the JSON response
        weather_data = response.json()
        return weather_data
        
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while accessing API for {city['city']}: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred for {city['city']}: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Request timeout occurred for {city['city']}: {timeout_err}")
    except requests.exceptions.RequestException as err:
        print(f"An unexpected error occurred during extraction for {city['city']}: {err}")
    
    # If extraction fails, return None to signal failure
    return None


# ==========================================
# L - Load Phase
# ==========================================

def connect_database():
    """Establishes a connection to the SQLite database file."""
    # We add this check to ensure the file is created if it doesn't exist
    print(f"--- Connecting to database: {DB_FILE} ---")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    return conn, cursor


def create_raw_table(cursor):
    """
    TODO #1: CREATE TABLE (IMPLEMENTED)
    
    Creates the 'raw_weather' table, designed to store the raw JSON 
    data extracted from the API.
    """
    print("--- Creating raw table 'raw_weather' ---")
    
    # Drop the table if it already exists to ensure we start fresh (idempotency)
    cursor.execute("DROP TABLE IF EXISTS raw_weather")
    
    # Columns:
    # id (PRIMARY KEY): Unique identifier for each record.
    # city, latitude, longitude (TEXT/REAL): Structured identification data.
    # raw_json (TEXT): A column to store the complete raw JSON response as text.
    cursor.execute("""
        CREATE TABLE raw_weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            latitude REAL,
            longitude REAL,
            raw_json TEXT
        )
    """)


def insert_raw_weather(cursor, city, weather_data):
    """
    TODO #2: INSERT INTO (IMPLEMENTED)
    
    Inserts the raw API weather data into the 'raw_weather' table.
    
    The 'raw_json' column requires the JSON data to be converted into a string format.
    """
    if weather_data is None:
        # If extraction failed, do not attempt to insert.
        print(f"Skipping insert for {city['city']} due to failed extraction.")
        return

    print(f"--- Loading raw data for {city['city']} into raw_weather ---")
    
    # Define the INSERT statement
    query = """
        INSERT INTO raw_weather (city, latitude, longitude, raw_json)
        VALUES (?, ?, ?, ?)
    """
    
    # Convert the JSON dictionary object into a formatted string for database storage
    json_string = json.dumps(weather_data)
    
    # Use a tuple for safe parameter binding
    cursor.execute(query, (city['city'], city['latitude'], city['longitude'], json_string))


# ==========================================
# T - Transform Phase (Within DB)
# ==========================================

def transform_data_inside_database(cursor):
    """
    In-Database Transformation. 
    
    Executes SQL to parse and flatten the raw JSON data stored in 'raw_weather',
    and creates a new, structured, optimized table named 'transformed_weather'.
    """
    print("--- Transforming raw JSON to structured table 'transformed_weather' ---")
    
    # Drop existing transformed tables
    cursor.execute("DROP TABLE IF EXISTS transformed_weather")

    # SQL command to create the final table structure from raw data
    # SQLite's json_extract() function is used to pull specific nested fields.
    cursor.execute("""
        CREATE TABLE transformed_weather AS
        SELECT
            id,
            city,
            latitude,
            longitude,
            -- Extracting current weather fields from the JSON string
            json_extract(raw_json, '$.current.time') AS observation_time,
            json_extract(raw_json, '$.current.temperature_2m') AS temperature_celsius,
            json_extract(raw_json, '$.current.relative_humidity_2m') AS humidity,
            json_extract(raw_json, '$.current.wind_speed_10m') AS wind_speed,
            json_extract(raw_json, '$.current.weather_code') AS weather_code
        FROM raw_weather
    """)


# ==========================================
# Presentation/Output
# ==========================================

def select_transformed_weather(cursor):
    """
    TODO #3: SELECT FROM (IMPLEMENTED)
    
    Selects and displays all finalized records from the 'transformed_weather' table.
    """
    print("\n--- Final Transformed Weather Report (Structured View) ---")
    
    cursor.execute("""
        SELECT 
            id, city, latitude, longitude, observation_time, 
            temperature_celsius, humidity, wind_speed, weather_code
        FROM transformed_weather
    """)
    rows = cursor.fetchall()
    
    # Define and print simple headers
    headers = ["ID", "City", "Lat.", "Lon.", "Obs. Time", "Temp. (C)", "Humid. (%)", "Wind (km/h)", "Code"]
    print(f"{headers[0]:<3} | {headers[1]:<10} | {headers[2]:<6} | {headers[3]:<6} | {headers[4]:<16} | {headers[5]:<9} | {headers[6]:<10} | {headers[7]:<11} | {headers[8]}")
    print("-" * 110)

    # Process each record and format the output
    count = 0
    for row in rows:
        r_id, city, lat, lon, time, temp, hum, wind, code = row
        # Formatting observation time for readability (optional, but good practice)
        formatted_time = time.replace("T", " ")
        print(f"{r_id:<3} | {city:<10} | {lat:6.3f} | {lon:6.3f} | {formatted_time:<16} | {temp:9.1f} | {hum:10d} | {wind:11.1f} | {code:<4}")
        count += 1
    
    if count == 0:
        print("No finalized records found. Pipeline execution may have failed.")
    print("-" * 110)


# ==========================================
# Main Execution Loop
# ==========================================

def main():
    print("==========================================")
    print("STARTING WEATHER ELT PIPELINE (PH)")
    print("==========================================\n")

    # Step 1: Connect to database
    conn, cursor = connect_database()

    # Step 2: Initialize Raw Table
    # (Extract Phase) Load Phase Part 1
    create_raw_table(cursor)

    # Clean existing raw data for a fresh run
    cursor.execute("DELETE FROM raw_weather")
    conn.commit()

    print()

    # Step 3: Iterate through cities, Extracting and Loading Raw JSON
    # This is the "L" phase: loading raw data from the external source
    extraction_count = 0
    for city in CITIES:
        # E - Extract raw data
        weather_data = extract_weather_data(city)
        
        if weather_data:
            # L - Load raw data into DB
            insert_raw_weather(cursor, city, weather_data)
            extraction_count += 1
        print()

    # Skip subsequent steps if all extractions failed
    if extraction_count == 0:
        print("Error: Extraction failed for all cities. Pipeline stopping.")
        conn.close()
        sys.exit(1)

    # Step 4: Finalize loading phase by committing raw inserts
    conn.commit()
    print("Raw data loading phase completed.\n")

    # Step 5: Execute In-Database Transformation
    # T - Transform data within the DB using SQL.
    # This creates 'transformed_weather' from 'raw_weather'.
    transform_data_inside_database(cursor)
    conn.commit()
    print("Transformation completed successfully.\n")

    # Step 6: Presentation / Output
    # This displays the end result of the ELT process.
    select_transformed_weather(cursor)

    # Step 7: Cleanup
    conn.close()
    print("\n==========================================")
    print("PIPELINE COMPLETED SUCCESSFULLY.")
    print("==========================================")

if __name__ == "__main__":
    main()
