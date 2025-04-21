import requests
import json
import os
import csv
from datetime import datetime, timedelta

"""
SI 206 Final Project: Weather History Cacher
Author: Ixsaael Hernandez
Date: 4-20-2025

This script fetches historical weather data from OpenWeatherMap History API,
stores up to 25 new hourly data points per run in a JSON cache, and exports it to CSV.
"""

def get_api_key(filename):
    try:
        with open(filename, 'r') as file:
            line = file.read().strip()
            if '=' in line:
                return line.split('=')[1].strip()
            return line
    except:
        return ""

API_KEY = get_api_key('api_base_weather_key.txt')

def get_json_content(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            return json.load(file)
    except:
        return {}

def save_cache(cache_dict, filename):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(cache_dict, file, indent=4)

def is_extreme_weather(entry):
    temp = entry.get("main", {}).get("temp", 0)
    humidity = entry.get("main", {}).get("humidity", 0)
    wind = entry.get("wind", {}).get("speed", 0)
    weather = entry.get("weather", [{}])[0].get("main", "").lower()

    extreme_weather_types = ["thunderstorm", "blizzard", "tornado"]

    return (
        temp > 35 or temp < -5 or
        humidity > 95 or
        wind > 30 or
        weather in extreme_weather_types
    )

def parse_history_data(data, city):
    results = []
    for entry in data.get("list", []):
        dt = datetime.utcfromtimestamp(entry.get("dt", 0)).strftime('%Y-%m-%d %H:%M:%S')
        parsed = {
            "city": city,
            "datetime": dt,
            "temp": entry.get("main", {}).get("temp"),
            "humidity": entry.get("main", {}).get("humidity"),
            "weather": entry.get("weather", [{}])[0].get("main", ""),
            "description": entry.get("weather", [{}])[0].get("description", ""),
            "wind_speed": entry.get("wind", {}).get("speed"),
            "is_extreme": int(is_extreme_weather(entry))
        }
        results.append(parsed)
    return results

def fetch_history(city, coords, start_ts, end_ts):
    url = "https://history.openweathermap.org/data/2.5/history/city"
    params = {
        "lat": coords["lat"],
        "lon": coords["lon"],
        "type": "hour",
        "start": start_ts,
        "end": end_ts,
        "appid": API_KEY,
        "units": "metric"
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ API call failed for {city}: {response.status_code} - {response.text}")
    return None

def build_cache_key(city, dt_str):
    return f"{city.lower().replace(' ', '_')}_{dt_str}"

def get_unix_range_list(start_date, num_days):
    ranges = []
    for i in range(num_days):
        current = start_date + timedelta(days=i)
        start_ts = int(current.timestamp())
        end_ts = int((current + timedelta(days=1)).timestamp())
        ranges.append((current.strftime('%Y-%m-%d'), start_ts, end_ts))
    return ranges

def export_cache_to_csv(cache_file, output_csv):
    cache = get_json_content(cache_file)
    if not cache:
        print("Nothing to export.")
        return

    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["city", "datetime", "temp", "humidity", "weather", "description", "wind_speed", "is_extreme"])
        writer.writeheader()
        for item in cache.values():
            writer.writerow(item)

def update_cache(city_dict, cache_file):
    cache = get_json_content(cache_file)
    if not isinstance(cache, dict):
        cache = {}

    # Multiple historical dates: May 21–27, 2023
    start_date = datetime(2025, 4, 9)
    date_ranges = get_unix_range_list(start_date, 2)

    added = 0

    for date_str, start_ts, end_ts in date_ranges:
        for city, coords in city_dict.items():
            if added >= 25:
                break

            data = fetch_history(city, coords, start_ts, end_ts)
            if data:
                parsed_list = parse_history_data(data, city)
                for item in parsed_list:
                    key = build_cache_key(city, item['datetime'])
                    if key not in cache and added < 25:
                        cache[key] = item
                        added += 1

    save_cache(cache, cache_file)
    return added

def main():
    cities_file = "cities_weather_coords.json"
    cache_file = "weather_cache.json"

    if not os.path.exists(cities_file):
        print("Missing 'cities_coords.json' with city coordinates.")
        return

    city_dict = get_json_content(cities_file)
    added = update_cache(city_dict, cache_file)

    print(f"\n✅ Finished caching process.")
    print(f"Total cities processed: {len(city_dict)}")
    print(f"New entries added: {added}")
    print(f"Cache file saved to: {cache_file}\n")

    export_cache_to_csv(cache_file, "weather_cache.csv")

if __name__ == "__main__":
    main()
