import os
from datetime import datetime, timedelta
from FP_hotel_database import BookingScraper 
from FP_weather_request import update_cache, get_json_content
from FP_weather_database import (
    set_up_database,
    create_weather_type_table,
    create_weather_data_table,
    load_weather_data,
    insert_weather_data
)
from FP_analyzer import WeatherHotelAnalyzer


def main():
    print("===== WEATHER-OR-NOT PROJECT =====")

    hotel_db = 'weather_hotel_data.db'
    cities = ["Detroit", "New York", "Chicago", "Miami", "Los Angeles"]
    check_in_date = "2025-04-22"
    check_out_date = "2025-04-23"

    scraper = BookingScraper(db_path=hotel_db)
    try:
        scraper.scrape_multiple_cities(cities, check_in_date, check_out_date)
    finally:
        scraper.close()

    print("\n=== Running Weather Cache Update ===")
    cities_coords_file = 'cities_weather_coords.json'
    cache_file = 'weather_cache.json'

    city_dict = get_json_content(cities_coords_file)
    new_entries = update_cache(city_dict, cache_file)
    print(f"Added {new_entries} new weather entries to {cache_file}")

    weather_db = 'weather.db'
    print("\n=== Ingesting Weather Cache into SQLite ===")
    if os.path.exists(weather_db):
        print(f"Removing old database: {weather_db}")
        os.remove(weather_db)

    cur, conn = set_up_database(weather_db)
    create_weather_type_table(cur, conn)
    create_weather_data_table(cur, conn)
    data = load_weather_data(cache_file)
    insert_weather_data(cur, conn, data)
    conn.close()
    print(f"Database initialized and populated: {weather_db}")

    print("\n=== Running Data Analysis ===")
    analyzer = WeatherHotelAnalyzer(
        hotel_db_path=hotel_db,
        weather_db_path=weather_db
    )
    try:
        analyzer.run_analysis()
    finally:
        analyzer.close()


if __name__ == "__main__":
    main()
