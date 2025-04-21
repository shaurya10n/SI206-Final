import sqlite3
import json
import os

# Mapping API weather descriptions to standardized types
WEATHER_MAPPING = {
    "clear": "Sunny",
    "clouds": "Cloudy",
    "rain": "Rainy",
    "snow": "Snowy",
    "mist": "Foggy",
    "haze": "Foggy",
    "thunderstorm": "Blizzard",
    "drizzle": "Rainy",
    "fog": "Foggy",
    "wind": "Windy"
}

def set_up_database(db_name):
    """
    Sets up SQLite database connection and returns cursor and connection.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    return cur, conn

def create_weather_type_table(cur, conn):
    """
    Creates the Weather_Type table with predefined weather categories.
    """
    weather_types = ["Sunny", "Rainy", "Snowy", "Windy", "Tornado", "Blizzard", "Cloudy", "Foggy", "Clear"]

    cur.execute("""
        CREATE TABLE IF NOT EXISTS Weather_Type (
            id INTEGER PRIMARY KEY, 
            title TEXT UNIQUE
        )
    """)

    for i, wt in enumerate(weather_types):
        cur.execute("INSERT OR IGNORE INTO Weather_Type (id, title) VALUES (?, ?)", (i, wt))

    conn.commit()

def create_weather_data_table(cur, conn):
    """
    Creates the Weather_Data table to store historical weather entries.
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Weather_Data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            datetime TEXT,
            temp REAL,
            humidity REAL,
            wind_speed REAL,
            description TEXT,
            weather_type_id INTEGER,
            is_extreme INTEGER,
            UNIQUE(city, datetime),
            FOREIGN KEY(weather_type_id) REFERENCES Weather_Type(id)
        )
    """)
    conn.commit()

def load_weather_data(filename):
    """
    Loads JSON weather data from a cache file.
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {}

def get_weather_type_id(cur, weather_type):
    """
    Normalizes weather type and returns corresponding Weather_Type ID.
    """
    normalized = WEATHER_MAPPING.get(weather_type.lower(), weather_type)
    cur.execute("SELECT id FROM Weather_Type WHERE LOWER(title) = ?", (normalized.lower(),))
    result = cur.fetchone()
    return result[0] if result else None

def insert_weather_data(cur, conn, data):
    """
    Inserts parsed weather data into the database.
    Prevents duplicates using city + datetime.
    Links to Weather_Type via foreign key.
    """
    count = 0
    for entry in data.values():
        city = entry.get("city")
        dt = entry.get("datetime")
        temp = entry.get("temp")
        humidity = entry.get("humidity")
        wind = entry.get("wind_speed")
        description = entry.get("description")
        weather = entry.get("weather")
        is_extreme = entry.get("is_extreme", 0)

        weather_type_id = get_weather_type_id(cur, weather)
        if weather_type_id is None:
            print(f"⚠️ Skipping unknown weather type: '{weather}' in {city} at {dt}")
            continue

        try:
            cur.execute("""
                INSERT OR IGNORE INTO Weather_Data 
                (city, datetime, temp, humidity, wind_speed, description, weather_type_id, is_extreme)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (city, dt, temp, humidity, wind, description, weather_type_id, is_extreme))
            count += 1
        except Exception as e:
            print(f"Insert failed for {city} @ {dt}: {e}")

    conn.commit()
    print(f"Inserted {count} new rows into Weather_Data.")

def main():
    """
    Main script to load weather cache and insert data into the database.
    """
    cur, conn = set_up_database("weather.db")
    create_weather_type_table(cur, conn)
    create_weather_data_table(cur, conn)
    
    data = load_weather_data("weather_cache.json")
    insert_weather_data(cur, conn, data)

if __name__ == "__main__":
    main()