import requests
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class APIClient:
    """Handles interaction with the external healthcare API."""

    def __init__(self, base_url):   
        self.base_url = base_url

    def fetch_data(self, country=None, start_date=None, end_date=None):
        """
        Fetches data from the specified CSV URL.
        For a CSV, filtering by country/date is done post-download.
        """
        try:
            logging.info(f"Attempting to fetch data from: {self.base_url}")
            response = requests.get(self.base_url, stream=True)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

            # Read CSV directly into a Pandas DataFrame
            df = pd.read_csv(response.iter_lines(decode_unicode=True), sep=',')
            logging.info(f"Successfully fetched {len(df)} raw records from API.")

            # Apply filters if provided (for CSV, this happens after download)
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

            return df.to_dict(orient='records') # Convert DataFrame to list of dictionaries
        except requests.exceptions.RequestException as e:
            logging.error(f"API Request Error: {e}")
            return None
        except pd.errors.EmptyDataError:
            logging.warning("API returned empty data or invalid CSV.")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred during API fetch: {e}")
            return None

