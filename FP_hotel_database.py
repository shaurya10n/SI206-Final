import requests
from bs4 import BeautifulSoup
import time
import random
import sqlite3
import pandas as pd
import re
from datetime import datetime, timedelta
import os

class BookingScraper:
    def __init__(self, db_path='weather_hotel_data.db'):
        """Initialize the scraper with headers and database connection"""
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/;q=0.8',
            'Connection': 'keep-alive',
            'Referer': 'https://www.booking.com/'
        }

        if os.path.exists(db_path):
            print(f"Removing old database: {db_path}")
            os.remove(db_path)

        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS hotels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hotel_name TEXT,
            location TEXT,
            price REAL,
            rating REAL,
            review_count INTEGER,
            scrape_date TEXT,
            check_in_date TEXT
        )
        ''')
        self.conn.commit()
        print(f"Database initialized: {db_path}")


    def generate_booking_url(self, city, check_in_date=None, check_out_date=None):
        """Generate a URL for Booking.com search results"""
        if not check_in_date:
            check_in_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        if not check_out_date:
            check_out_date = (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d')

        city_formatted = city.replace(' ', '+')

        url = (f"https://www.booking.com/searchresults.html?"
               f"ss={city_formatted}&"
               f"checkin={check_in_date}&"
               f"checkout={check_out_date}&"
               f"group_adults=1&no_rooms=1&"
               f"aid=304142&label=gen173nr-1FCAEoggI46AdIM1gEaJsCiAEBmAExuAEHyAEM2AEB6AEB-AECiAIBqAIDuAKGtJvABsACAdICJDc3MTAxODk3LThjMTEtNDM3Ni1hNGUxLWM2YjkxODhlOGQ4NNgCBeACAQ") 
        return url

    def fetch_page(self, url):
        """Fetch the HTML content of a page with politeness delay"""
        try:
            sleep_time = random.uniform(5, 7)
            print(f"Waiting for {sleep_time:.2f} seconds before fetching...")
            time.sleep(sleep_time)

            response = requests.get(url, headers=self.headers)
            response.raise_for_status() 
            time.sleep(random.uniform(1, 3))
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {url}: {e}")
            return None

    def parse_hotels(self, html_content, city, check_in_date):
        """Parse hotel information from HTML content"""
        if not html_content:
            print("No HTML content to parse.")
            return []

        soup = BeautifulSoup(html_content, 'html.parser')
        hotels_data = []

        hotel_containers = soup.select('div[data-testid="property-card"]')

        if not hotel_containers:
            print("No hotel containers found with the specified selector.")
            hotel_containers = soup.select('div.c624d7469d.a0e60936ad.a3214e5942.b0db0e8ada')
            if not hotel_containers:
                 print("No hotel containers found even with the fallback selector.")

        for i, hotel in enumerate(hotel_containers):
            try:

                hotel_name_elem = hotel.select_one('div[data-testid="title"]')
                if not hotel_name_elem:
                     hotel_name_elem = hotel.select_one('div.f6431b446c.a15b38c233')

                hotel_name = hotel_name_elem.text.strip() if hotel_name_elem else "Unknown"

                location = city

                price_element = hotel.select_one('[data-testid="price-and-discounted-price"]')

                price = 0.0

                if price_element:
                    price_text = price_element.get_text(strip=True)

                    price_match = re.search(r'[\d,.]+', price_text)

                    if price_match:
                         price_str = price_match.group(0)
                         price = float(price_str.replace(',', ''))
                
                rating_elem = hotel.select_one('div.a3b8729ab1.d86cee9b25')
                if not rating_elem:
                     rating_elem = hotel.select_one('div[data-testid="review-score"]')

                rating = 0.0
                if rating_elem:
                    rating_text = rating_elem.text.strip()
                    rating_match = re.search(r'(\d+(\.\d+)?)$', rating_text)
                    if rating_match:
                        rating = float(rating_match.group(1))

                review_count_elem = hotel.select_one('div.abf093bdfe.f45d8e4c32.d935416c47') 
                if not review_count_elem:
                    review_count_elem = hotel.select_one('div[data-testid="review-score"] div:last-child')

                review_count = 0
                if review_count_elem:
                    review_text = review_count_elem.text.strip()
                    review_count_match = re.search(r'\d{1,3}(?:,\d{3})*', review_text.replace('.', ''))
                    if review_count_match:
                         review_count = int(review_count_match.group(0).replace(',', ''))

                hotels_data.append({
                    'hotel_name': hotel_name,
                    'location': location,
                    'price': price,
                    'rating': rating,
                    'review_count': review_count,
                    'scrape_date': datetime.now().strftime('%Y-%m-%d'),
                    'check_in_date': check_in_date
                })

            except Exception as e:
                print(f"Error parsing hotel container {i+1}: {e}")
                continue 

        return hotels_data

    def save_to_db(self, hotels_data):
        """Save hotel data to the database"""
        if not hotels_data:
            print("No hotel data to save")
            return 0

        count = 0
        for hotel in hotels_data:
            try:
                self.cursor.execute('''
                INSERT INTO hotels (hotel_name, location, price, rating, review_count, scrape_date, check_in_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    hotel['hotel_name'],
                    hotel['location'],
                    hotel['price'],
                    hotel['rating'],
                    hotel['review_count'],
                    hotel['scrape_date'],
                    hotel['check_in_date']
                ))
                count += 1
            except sqlite3.Error as e:
                print(f"Database error saving hotel {hotel.get('hotel_name', 'Unknown')}: {e}")
                continue

        self.conn.commit()
        return count

    def scrape_city(self, city, check_in_date=None, check_out_date=None):
        """Scrape hotel data for a specific city"""
        print(f"\n--- Scraping hotels for {city} ({check_in_date} to {check_out_date}) ---")

        url = self.generate_booking_url(city, check_in_date, check_out_date)

        html_content = self.fetch_page(url)

        effective_check_in_date = check_in_date or (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

        hotels_data = self.parse_hotels(html_content, city, effective_check_in_date)

        saved_count = self.save_to_db(hotels_data)
        print(f"Finished scraping {city}. Saved {saved_count} hotels.")

        return hotels_data

    def scrape_multiple_cities(self, cities, check_in_date=None, check_out_date=None):
        """Scrape hotel data for multiple cities"""
        all_hotels = []
        for city in cities:
            city_hotels = self.scrape_city(city, check_in_date, check_out_date)
            all_hotels.extend(city_hotels)
        return all_hotels

    def get_hotels_data_from_db(self, location=None, check_in_date=None, limit=10):
        """Query hotel data from database with optional filters"""
        query = "SELECT * FROM hotels"
        params = []

        conditions = []
        if location:
            conditions.append("location = ?")
            params.append(location)
        if check_in_date:
            conditions.append("check_in_date = ?")
            params.append(check_in_date)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += f" LIMIT {limit}"

        print(f"\nQuerying DB: {query} with params {params}")
        self.cursor.execute(query, params)
        rows = self.cursor.fetchall()

        columns = [description[0] for description in self.cursor.description]
        df = pd.DataFrame(rows, columns=columns)

        return df

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

#Testing code
def main():
    db_file = 'weather_hotel_data.db'
    scraper = BookingScraper(db_path=db_file)

    cities = ["Detroit", "New York", "Chicago", "Miami", "Los Angeles"]
    check_in_date = "2025-04-22"
    check_out_date = "2025-04-23" 

    try:
        all_hotels = scraper.scrape_multiple_cities(cities, check_in_date, check_out_date)

        print(f"\n--- Scraping Complete ---")

    except Exception as e:
        print(f"\nAn error occurred during the scraping process: {e}")

    finally:
        scraper.close()

if __name__ == "__main__":
    main()