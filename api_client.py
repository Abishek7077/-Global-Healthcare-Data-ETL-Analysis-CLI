import requests
import logging
import pandas as pd
from io import StringIO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class APIClient:
    """Handles interaction with the external healthcare API (CSV source)."""

    def __init__(self, base_url):
        self.base_url = base_url

    def fetch_data(self, country=None, start_date=None, end_date=None):
        """
        Fetches the CSV from base_url, parses it into a DataFrame, and applies
        optional filters for country and date range.
        """
        try:
            logging.info(f"Attempting to fetch data from: {self.base_url}")
            response = requests.get(self.base_url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"API Request Error: {e}")
            return None

        try:
            # Use StringIO so pandas gets a proper buffer (not a generator)
            df = pd.read_csv(StringIO(response.text))
            logging.info(f"Successfully fetched {len(df)} raw records from API.")
        except pd.errors.EmptyDataError:
            logging.warning("API returned empty data or invalid/empty CSV.")
            return None
        except Exception as e:
            logging.error(f"Failed to parse CSV into DataFrame: {e}")
            return None

        # Validate expected columns
        if 'location' not in df.columns or 'date' not in df.columns:
            logging.error("Fetched CSV missing required columns 'location' or 'date'.")
            return None

        # Apply filters
        if country:
            df = df[df['location'].str.lower() == country.lower()]
            logging.info(f"Filtered data for country: {country}. Records remaining: {len(df)}")
        if start_date:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df[df['date'] >= pd.to_datetime(start_date)]
            logging.info(f"Filtered data from start date: {start_date}. Records remaining: {len(df)}")
        if end_date:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df[df['date'] <= pd.to_datetime(end_date)]
            logging.info(f"Filtered data up to end date: {end_date}. Records remaining: {len(df)}")

        # Drop rows where date parsing failed
        if 'date' in df.columns:
            df = df.dropna(subset=['date'])

        return df.to_dict(orient='records')
        
        
