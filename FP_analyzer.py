import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

class WeatherHotelAnalyzer:
    def __init__(self, hotel_db_path='weather_hotel_data.db', weather_db_path='weather.db'):
        """Initialize analyzer with paths to both databases"""
        self.hotel_db_path = hotel_db_path
        self.weather_db_path = weather_db_path
        
        if not os.path.exists(hotel_db_path):
            raise FileNotFoundError(f"Hotel database not found: {hotel_db_path}")
        if not os.path.exists(weather_db_path):
            raise FileNotFoundError(f"Weather database not found: {weather_db_path}")
        
        self.hotel_conn = sqlite3.connect(hotel_db_path)
        self.weather_conn = sqlite3.connect(weather_db_path)
        
        os.makedirs('charts', exist_ok=True)
        
    def close(self):
        """Close all database connections"""
        if hasattr(self, 'hotel_conn'):
            self.hotel_conn.close()
        if hasattr(self, 'weather_conn'):
            self.weather_conn.close()
            
    def get_merged_data(self):
        """Join hotel and weather data based on location and date"""
        hotel_df = pd.read_sql_query('''
            SELECT hotel_name, location, price, rating, 
                   review_count, scrape_date, check_in_date
            FROM hotels
        ''', self.hotel_conn)
        
        weather_df = pd.read_sql_query('''
            SELECT wd.city as location, wd.datetime, wd.temp, 
                   wd.humidity, wd.wind_speed, wd.description,
                   wt.title as weather_type, wd.is_extreme
            FROM Weather_Data wd
            JOIN Weather_Type wt ON wd.weather_type_id = wt.id
        ''', self.weather_conn)
        
        hotel_df['check_in_date'] = pd.to_datetime(hotel_df['check_in_date']).dt.date
        weather_df['date'] = pd.to_datetime(weather_df['datetime']).dt.date
        
        merged_df = pd.merge(
            hotel_df,
            weather_df,
            left_on=['location', 'check_in_date'],
            right_on=['location', 'date'],
            how='inner'
        )
        
        return merged_df
    
    def plot_price_by_weather(self, save_path='charts/price_by_weather.png'):
        """Bar chart of average hotel price and temperature for each city"""
        df = self.get_merged_data()
        if df.empty:
            print("No data available for price-by-weather plot.")
            return

        stats = df.groupby('location').agg({'price': 'mean', 'temp': 'mean'}).reset_index()

        x = np.arange(len(stats['location']))
        width = 0.35

        fig, ax1 = plt.subplots()
        ax1.bar(x - width/2, stats['price'], width, label='Avg Price')
        ax1.set_xlabel('City')
        ax1.set_ylabel('Average Hotel Price')
        ax1.set_xticks(x)
        ax1.set_xticklabels(stats['location'], rotation=45)

        ax2 = ax1.twinx()
        ax2.bar(x + width/2, stats['temp'], width, label='Avg Temperature')
        ax2.set_ylabel('Average Temperature (°C)')

        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')

        plt.title('Average Hotel Price and Temperature by City')
        plt.tight_layout()
        plt.savefig(save_path)
        plt.close()
        print(f"Saved bar chart to {save_path}")
        
    def plot_price_temp_scatter(self, save_path='charts/price_temp_scatter.png'):
        """Scatter plot of hotel price vs temperature for all hotels"""
        df = self.get_merged_data()
        if df.empty:
            print("No data available for price-temp scatter plot.")
            return

        plt.figure()
        plt.scatter(df['temp'], df['price'])
        plt.xlabel('Temperature (°C)')
        plt.ylabel('Hotel Price')
        plt.title('Hotel Price vs Temperature')
        plt.tight_layout()
        plt.savefig(save_path)
        plt.close()
        print(f"Saved scatter plot to {save_path}")

    def plot_price_temp_line(self, save_path='charts/price_temp_line.png'):
        """Line chart showing average hotel price by temperature"""
        df = self.get_merged_data()
        if df.empty:
            print("No data available for price-temp line plot.")
            return

        temp_stats = df.groupby('temp').agg({'price': 'mean'}).reset_index().sort_values('temp')

        plt.figure()
        plt.plot(temp_stats['temp'], temp_stats['price'])
        plt.xlabel('Temperature (°C)')
        plt.ylabel('Average Hotel Price')
        plt.title('Average Hotel Price by Temperature')
        plt.tight_layout()
        plt.savefig(save_path)
        plt.close()
        print(f"Saved line chart to {save_path}")
        
    def analyze_price_by_weather_condition(self):
        """Calculate average hotel price per city for different weather conditions."""
        df = self.get_merged_data()
        if df.empty:
            print("No data available for price-by-weather-condition analysis.")
            return

        weather_stats = df.groupby(['location', 'weather_type']).agg(
            avg_price=pd.NamedAgg(column='price', aggfunc='mean')
        ).reset_index()

        print("\nANALYSIS: Average Hotel Price per City for Different Weather Conditions\n")
        for _, row in weather_stats.iterrows():
            print(f"{row['location']} ({row['weather_type']}): Avg Price = ${row['avg_price']:.2f}")

    def analyze_temp_price_correlation(self):
        """Perform correlation analysis between temperature and hotel prices."""
        df = self.get_merged_data()
        if df.empty:
            print("No data available for temperature-price correlation analysis.")
            return

        correlation = df['price'].corr(df['temp'])
        print("\nANALYSIS: Correlation Between Temperature and Hotel Prices\n")
        print(f"Correlation coefficient: {correlation:.2f}")

    def analyze_weather_impact_on_pricing(self):
        """Analyze the impact of weather conditions on hotel pricing."""
        df = self.get_merged_data()
        if df.empty:
            print("No data available for weather impact analysis.")
            return

        weather_impact = df.groupby('weather_type').agg(
            avg_price=pd.NamedAgg(column='price', aggfunc='mean')
        ).reset_index()

        print("\nANALYSIS: Impact of Weather Conditions on Hotel Pricing\n")
        for _, row in weather_impact.iterrows():
            print(f"{row['weather_type']}: Avg Price = ${row['avg_price']:.2f}")

    def generate_all_visualizations(self):
        """Helper to generate all charts"""
        self.plot_price_by_weather()
        self.plot_price_temp_scatter()
        self.plot_price_temp_line()

    def run_analysis(self):
        """Run all analyses and print detailed results."""
        print("\n===== WEATHER-OR-NOT ANALYSIS RESULTS =====\n")
        merged_df = self.get_merged_data()
        if merged_df.empty:
            print("No merged data available for analysis.")
            return

        # Analysis 1: Average hotel price and temperature by city
        print("ANALYSIS 1: Average Hotel Price and Temperature by City\n")
        city_stats = merged_df.groupby('location').agg(
            avg_price=pd.NamedAgg(column='price', aggfunc='mean'),
            avg_temp=pd.NamedAgg(column='temp', aggfunc='mean')
        )
        for city, row in city_stats.iterrows():
            print(f"{city}: Avg Price = ${row['avg_price']:.2f}, Avg Temp = {row['avg_temp']:.1f}°C")
        print()

        # Analysis 2: Temperature and hotel price correlation
        print("ANALYSIS 2: Temperature and Hotel Price Correlation\n")
        corr = merged_df['price'].corr(merged_df['temp'])
        print(f"Correlation coefficient between temperature and hotel price: {corr:.2f}\n")

        self.analyze_price_by_weather_condition()
        self.analyze_temp_price_correlation()
        self.analyze_weather_impact_on_pricing()

        print("\nGenerating visualizations...")
        self.generate_all_visualizations()
        print("\nAnalysis complete!")
