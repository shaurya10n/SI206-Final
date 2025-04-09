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
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
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
    
    def generate_booking_url(self, city, check_in_date=None, check_out_date=None):
        """Generate a URL for Booking.com search results"""
        if not check_in_date:
            check_in_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        if not check_out_date:
            check_out_date = (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d')
            
        city_formatted = city.replace(' ', '+')
        
        url = f"https://www.booking.com/searchresults.html?ss={city_formatted}&checkin={check_in_date}&checkout={check_out_date}&group_adults=1&no_rooms=1"
        return url
    
    def fetch_page(self, url):
        """Fetch the HTML content of a page"""
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page: {e}")
            return None
    
    def parse_hotels(self, html_content, city, check_in_date):
        """Parse hotel information from HTML content"""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        hotels_data = []
        
        hotel_containers = soup.select('div.c624d7469d.a0e60936ad.a3214e5942.b0db0e8ada')
        
        for hotel in hotel_containers:
            try:
                hotel_name_elem = hotel.select_one('div.f6431b446c.a15b38c233')
                hotel_name = hotel_name_elem.text.strip() if hotel_name_elem else "Unknown"
                
                location = city
                
                price = 0.0
                price_selectors = [
                    'span.f6431b446c.fbfd7c1165.e84eb96b1f',
                    'span[data-testid="price-and-discounted-price"]',
                    'div.c5ca594cb1 span',
                    'div.c5ca594cb1', 
                ]
                
                for selector in price_selectors:
                    price_elem = hotel.select_one(selector)
                    if price_elem:
                        price_text = price_elem.text.strip()
                        price_numbers = re.findall(r'\d+', price_text)
                        if price_numbers:
                            price = float(''.join(price_numbers))
                            break
                
                rating_elem = hotel.select_one('div.a3b8729ab1.d86cee9b25')
                if rating_elem:
                    rating_text = rating_elem.text.strip()
                    rating_match = re.search(r'(\d+\.\d+)$', rating_text)
                    if rating_match:
                        rating = float(rating_match.group(1))
                    else:
                        numbers = re.findall(r'\d+\.\d+', rating_text)
                        rating = float(numbers[-1]) if numbers else 0.0
                else:
                    rating = 0.0
                
                review_count_elem = hotel.select_one('div.abf093bdfe.f45d8e4c32.d935416c47')
                if review_count_elem:
                    review_text = review_count_elem.text.strip()
                    review_count = int(''.join(c for c in review_text if c.isdigit())) if any(c.isdigit() for c in review_text) else 0
                else:
                    review_count = 0
                
                print(f"Hotel: {hotel_name}, Extracted Price: {price}")
                
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
                print(f"Error parsing hotel: {e}")
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
                print(f"Saving to DB - Hotel: {hotel['hotel_name']}, Price: {hotel['price']}")
                
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
                print(f"Database error: {e}")
                continue
        
        self.conn.commit()
        return count
    
    def scrape_city(self, city, check_in_date=None, check_out_date=None):
        """Scrape hotel data for a specific city"""
        print(f"Scraping hotels for {city}...")
        
        url = self.generate_booking_url(city, check_in_date, check_out_date)
        
        html_content = self.fetch_page(url)
        
        hotels_data = self.parse_hotels(html_content, city, check_in_date or (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d'))
        
        saved_count = self.save_to_db(hotels_data)
        print(f"Saved {saved_count} hotels for {city}")
        
        time.sleep(random.uniform(2, 5))
        
        return hotels_data
    
    def scrape_multiple_cities(self, cities, check_in_date=None, check_out_date=None):
        """Scrape hotel data for multiple cities"""
        all_hotels = []
        for city in cities:
            city_hotels = self.scrape_city(city, check_in_date, check_out_date)
            all_hotels.extend(city_hotels)
            time.sleep(random.uniform(5, 10))
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
        
        self.cursor.execute(query, params)
        rows = self.cursor.fetchall()
        
        columns = ['id', 'hotel_name', 'location', 'price', 'rating', 'review_count', 'scrape_date', 'check_in_date']
        df = pd.DataFrame(rows, columns=columns)
        
        return df
    
    def check_db_contents(self):
        """Debug function to check database contents"""
        self.cursor.execute("SELECT COUNT(*) FROM hotels")
        count = self.cursor.fetchone()[0]
        print(f"Total records in database: {count}")
        
        self.cursor.execute("SELECT COUNT(*) FROM hotels WHERE price > 0")
        nonzero_price_count = self.cursor.fetchone()[0]
        print(f"Records with non-zero prices: {nonzero_price_count}")
        
        self.cursor.execute("SELECT hotel_name, price FROM hotels LIMIT 10")
        samples = self.cursor.fetchall()
        print("Sample prices:")
        for name, price in samples:
            print(f"  {name}: ${price}")
    
    def close(self):
        """Close database connection"""
        self.conn.close()

#Testing code
if __name__ == "__main__":
    scraper = BookingScraper()
    
    cities = ["Detroit", "New York", "Chicago", "Miami", "Los Angeles"]
    check_in_date = "2025-04-09"
    check_out_date = "2025-04-10"
    
    try:
        all_hotels = scraper.scrape_multiple_cities(cities, check_in_date, check_out_date)
        
        print(f"Scraped {len(all_hotels)} hotels in total")
        
        scraper.check_db_contents()
        
        print("\nDataFrame from database:")
        df = scraper.get_hotels_data_from_db()
        print(df[['hotel_name', 'location', 'price', 'rating']])
        
    finally:
        scraper.close()