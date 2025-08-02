# import argparse
# import logging
# import configparser
# from datetime import datetime, timedelta
# from tabulate import tabulate # For pretty printing tables

# from api_client import APIClient
# from data_transformer import DataTransformer
# from mysql_handler import MySQLHandler

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# class CLIManager:
#     """Manages the command-line interface."""

#     def __init__(self, api_client, db_handler):
#         self.api_client = api_client
#         self.db_handler = db_handler
#         self.parser = argparse.ArgumentParser(description="Global Healthcare Data ETL & Analysis CLI")
#         self._setup_parser()

#     def _setup_parser(self):
#         subparsers = self.parser.add_subparsers(dest='command', help='Available commands')

#         # ETL Command
#         fetch_parser = subparsers.add_parser('fetch_data', help='Fetch, transform, and load healthcare data.')
#         fetch_parser.add_argument('--country', type=str, help='Country to fetch data for (e.g., "India")', required=True)
#         fetch_parser.add_argument('--start_date', type=str, help='Start date (YYYY-MM-DD)')
#         fetch_parser.add_argument('--end_date', type=str, help='End date (YYYY-MM-DD)')

#         # Query Commands
#         query_parser = subparsers.add_parser('query_data', help='Query loaded data.')
#         query_subparsers = query_parser.add_subparsers(dest='query_type', help='Types of queries')

#         total_cases_parser = query_subparsers.add_parser('total_cases', help='Get total cases for a country.')
#         total_cases_parser.add_argument('country', type=str, help='Country name.')

#         daily_trends_parser = query_subparsers.add_parser('daily_trends', help='Get daily trends for a metric.')
#         daily_trends_parser.add_argument('country', type=str, help='Country name.')
#         daily_trends_parser.add_argument('metric', type=str, help='Metric (e.g., "new_cases", "new_deaths").')

#         top_n_parser = query_subparsers.add_parser('top_n_countries_by_metric', help='Get top N countries by a metric.')
#         top_n_parser.add_argument('n', type=int, help='Number of top countries.')
#         top_n_parser.add_argument('metric', type=str, help='Metric (e.g., "total_cases", "total_vaccinations").')

#         # DB Management Commands
#         subparsers.add_parser('list_tables', help='List tables in the database.')
#         subparsers.add_parser('drop_tables', help='Drop all created tables (USE WITH CAUTION).')

#     def run(self):
#         args = self.parser.parse_args()

#         if args.command == 'fetch_data':
#             self._fetch_and_load_data(args.country, args.start_date, args.end_date)
#         elif args.command == 'query_data':
#             self._execute_query(args)
#         elif args.command == 'list_tables':
#             self._list_db_tables()
#         elif args.command == 'drop_tables':
#             self._drop_db_tables()
#         else:
#             self.parser.print_help()

#     def _fetch_and_load_data(self, country, start_date, end_date):
#         logging.info(f"Fetching data for {country} from {start_date or 'beginning'} to {end_date or 'now'}")
#         raw_data = self.api_client.fetch_data(country, start_date, end_date)

#         if raw_data is None or not raw_data:
#             logging.error("Failed to fetch raw data or no data available for the specified criteria.")
#             return

#         transformer = DataTransformer()

#         # Process for daily_cases table
#         logging.info("Transforming data for 'daily_cases' table...")
#         transformed_cases_df = transformer.clean_and_transform(raw_data, 'daily_cases')
#         if not transformed_cases_df.empty:
#             self.db_handler.insert_data('daily_cases', transformed_cases_df.to_dict(orient='records'))
#         else:
#             logging.warning("No data to load into 'daily_cases' after transformation.")

#         # Process for vaccination_data table
#         logging.info("Transforming data for 'vaccination_data' table...")
#         transformed_vacc_df = transformer.clean_and_transform(raw_data, 'vaccination_data')
#         if not transformed_vacc_df.empty:
#             self.db_handler.insert_data('vaccination_data', transformed_vacc_df.to_dict(orient='records'))
#         else:
#             logging.warning("No data to load into 'vaccination_data' after transformation.")

#         logging.info("Data fetching and loading process completed.")

#     def _execute_query(self, args):
#         results = None
#         headers = []

#         if args.query_type == 'total_cases':
#             sql_query = """
#             SELECT country_name, SUM(total_cases) AS total_cases
#             FROM daily_cases
#             WHERE country_name = %s
#             GROUP BY country_name;
#             """
#             results = self.db_handler.query_data(sql_query, (args.country,))
#             headers = ["Country", "Total Cases"]
#             if results:
#                 print(f"\nTotal COVID-19 Cases in {args.country}: {results[0]['total_cases']:,}")
#             else:
#                 print(f"No data found for total cases in {args.country}.")

#         elif args.query_type == 'daily_trends':
#             if args.metric not in ['new_cases', 'new_deaths']:
#                 logging.error(f"Invalid metric for daily trends: {args.metric}. Choose 'new_cases' or 'new_deaths'.")
#                 return

#             sql_query = f"""
#             SELECT report_date, {args.metric}
#             FROM daily_cases
#             WHERE country_name = %s
#             ORDER BY report_date ASC;
#             """
#             results = self.db_handler.query_data(sql_query, (args.country,))
#             headers = ["Date", args.metric.replace('_', ' ').title()]
#             if results:
#                 print(f"\nDaily {args.metric.replace('_', ' ').title()} Trends for {args.country}:")
#                 print(tabulate(results, headers=headers, tablefmt="grid"))
#             else:
#                 print(f"No daily trends found for {args.country} for metric {args.metric}.")

#         elif args.query_type == 'top_n_countries_by_metric':
#             if args.metric not in ['total_cases', 'total_deaths', 'total_vaccinations']:
#                 logging.error(f"Invalid metric for top N countries: {args.metric}. Choose 'total_cases', 'total_deaths', or 'total_vaccinations'.")
#                 return

#             table_name = 'daily_cases' if args.metric in ['total_cases', 'total_deaths'] else 'vaccination_data'
            
#             # For total_cases/deaths, we need the latest cumulative value per country
#             # For total_vaccinations, we need the latest cumulative value per country
#             sql_query = f"""
#             SELECT country_name, MAX({args.metric}) AS latest_metric_value
#             FROM {table_name}
#             GROUP BY country_name
#             ORDER BY latest_metric_value DESC
#             LIMIT %s;
#             """
#             results = self.db_handler.query_data(sql_query, (args.n,))
#             headers = ["Rank", "Country", args.metric.replace('_', ' ').title()]
            
#             if results:
#                 print(f"\nTop {args.n} Countries by {args.metric.replace('_', ' ').title()}:")
#                 formatted_results = []
#                 for i, row in enumerate(results):
#                     formatted_results.append([i + 1, row['country_name'], f"{row['latest_metric_value']:,}"])
#                 print(tabulate(formatted_results, headers=headers, tablefmt="grid"))
#             else:
#                 print(f"No data found for top {args.n} countries by {args.metric}.")
#         else:
#             logging.warning("Unknown query type.")

#     def _list_db_tables(self):
#         tables = self.db_handler.list_tables()
#         if tables:
#             print("\nTables in the database:")
#             for table in tables:
#                 print(f"- {table}")
#         else:
#             print("No tables found in the database.")

#     def _drop_db_tables(self):
#         confirm = input("Are you sure you want to drop all created tables? This action cannot be undone. (yes/no): ")
#         if confirm.lower() == 'yes':
#             if self.db_handler.drop_tables():
#                 print("Tables dropped successfully.")
#             else:
#                 print("Failed to drop tables.")
#         else:
#             print("Table dropping cancelled.")

# def main():
#     # Load database and API configuration
#     config = configparser.ConfigParser()
#     try:
#         config.read('config.ini')
#         db_config = dict(config['DATABASE'])
#         api_base_url = config['API']['base_url']
#     except KeyError as e:
#         logging.error(f"Missing configuration section or key in config.ini: {e}")
#         logging.error("Please ensure config.ini has [DATABASE] and [API] sections with required keys.")
#         return
#     except Exception as e:
#         logging.error(f"Error reading config.ini: {e}")
#         return

#     # Initialize handlers
#     db_handler = MySQLHandler(db_config)
#     api_client = APIClient(api_base_url)

#     # Ensure tables exist on startup
#     if not db_handler.create_tables():
#         logging.error("Failed to create or verify database tables. Exiting.")
#         db_handler.close()
#         return

#     cli_manager = CLIManager(api_client, db_handler)
#     cli_manager.run()

#     db_handler.close()

# if __name__ == "__main__":
#     main()



# PATCHED: Complete ETL modules (main.py, api_client.py, data_transformer.py, mysql_handler.py)

# === main.py ===
import argparse
import logging
import configparser
from tabulate import tabulate

from api_client import APIClient
from data_transformer import DataTransformer
from mysql_handler import MySQLHandler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CLIManager:
    def __init__(self, api_client, db_handler):
        self.api_client = api_client
        self.db_handler = db_handler
        self.parser = argparse.ArgumentParser(description="Global Healthcare Data ETL & Analysis CLI")
        self._setup_parser()

    def _setup_parser(self):
        subparsers = self.parser.add_subparsers(dest='command', help='Available commands')

        fetch_parser = subparsers.add_parser('fetch_data', help='Fetch, transform, and load healthcare data.')
        fetch_parser.add_argument('--country', type=str, required=True, help='Country to fetch data for')
        fetch_parser.add_argument('--start_date', type=str, help='Start date YYYY-MM-DD')
        fetch_parser.add_argument('--end_date', type=str, help='End date YYYY-MM-DD')

        query_parser = subparsers.add_parser('query_data', help='Query loaded data.')
        query_subparsers = query_parser.add_subparsers(dest='query_type', help='Types of queries')

        total_cases_parser = query_subparsers.add_parser('total_cases', help='Get total cases for a country.')
        total_cases_parser.add_argument('country', type=str)

        daily_trends_parser = query_subparsers.add_parser('daily_trends', help='Get daily trends for a metric.')
        daily_trends_parser.add_argument('country', type=str)
        daily_trends_parser.add_argument('metric', type=str, choices=['new_cases', 'new_deaths'])

        top_n_parser = query_subparsers.add_parser('top_n_countries_by_metric', help='Get top N countries by metric.')
        top_n_parser.add_argument('n', type=int)
        top_n_parser.add_argument('metric', type=str, choices=['total_cases', 'total_deaths', 'total_vaccinations'])

        subparsers.add_parser('list_tables', help='List tables in the database.')
        subparsers.add_parser('drop_tables', help='Drop all created tables.')

    def run(self):
        args = self.parser.parse_args()
        if args.command == 'fetch_data':
            self._fetch_and_load_data(args.country, args.start_date, args.end_date)
        elif args.command == 'query_data':
            self._execute_query(args)
        elif args.command == 'list_tables':
            self._list_db_tables()
        elif args.command == 'drop_tables':
            self._drop_db_tables()
        else:
            self.parser.print_help()

    def _fetch_and_load_data(self, country, start_date, end_date):
        logging.info(f"Fetching data for {country} from {start_date or 'beginning'} to {end_date or 'now'}")
        raw_data = self.api_client.fetch_data(country=country, start_date=start_date, end_date=end_date)

        if not raw_data:
            logging.error("No data returned from API.")
            return

        transformer = DataTransformer()

        logging.info("Transforming data for 'daily_cases'")
        daily_cases_df = transformer.clean_and_transform(raw_data, 'daily_cases')
        if not daily_cases_df.empty:
            self.db_handler.insert_data('daily_cases', daily_cases_df.to_dict(orient='records'))
        else:
            logging.warning("No daily_cases data to insert after transformation.")

        logging.info("Transforming data for 'vaccination_data'")
        vaccination_df = transformer.clean_and_transform(raw_data, 'vaccination_data')
        if not vaccination_df.empty:
            self.db_handler.insert_data('vaccination_data', vaccination_df.to_dict(orient='records'))
        else:
            logging.warning("No vaccination_data to insert after transformation.")

        logging.info("Data load complete.")

    def _execute_query(self, args):
        if args.query_type == 'total_cases':
            sql_query = """
            SELECT country_name, SUM(total_cases) AS total_cases
            FROM daily_cases
            WHERE country_name = %s
            GROUP BY country_name;
            """
            results = self.db_handler.query_data(sql_query, (args.country,))
            if results:
                print(f"Total COVID-19 Cases in {args.country}: {results[0]['total_cases']:,}")
            else:
                print(f"No data found for total cases in {args.country}.")
        elif args.query_type == 'daily_trends':
            sql_query = f"""
            SELECT report_date, {args.metric}
            FROM daily_cases
            WHERE country_name = %s
            ORDER BY report_date ASC;
            """
            results = self.db_handler.query_data(sql_query, (args.country,))
            if results:
                print(f"Daily {args.metric.replace('_',' ').title()} Trends for {args.country}:")
                print(tabulate(results, headers=["Date", args.metric.replace('_',' ').title()], tablefmt='grid'))
            else:
                print(f"No daily trends found for {args.country} for metric {args.metric}.")

        elif args.query_type == 'top_n_countries_by_metric':
            table_name = 'daily_cases' if args.metric in ['total_cases', 'total_deaths'] else 'vaccination_data'
            sql_query = f"""
            SELECT country_name, MAX({args.metric}) AS latest_metric_value
            FROM {table_name}
            GROUP BY country_name
            ORDER BY latest_metric_value DESC
            LIMIT %s;
            """
            results = self.db_handler.query_data(sql_query, (args.n,))
            if results:
                formatted = []
                for i, row in enumerate(results):
                    formatted.append([i + 1, row['country_name'], f"{row['latest_metric_value']:,}"])
                print(f"Top {args.n} Countries by {args.metric.replace('_',' ').title()}:")
                print(tabulate(formatted, headers=["Rank", "Country", args.metric.replace('_',' ').title()], tablefmt='grid'))
            else:
                print(f"No data found for top {args.n} countries by {args.metric}.")
        else:
            logging.warning("Unknown query type.")

    def _list_db_tables(self):
        tables = self.db_handler.list_tables()
        if tables:
            print("Tables in the database:")
            for t in tables:
                print(f"- {t}")
        else:
            print("No tables found in the database.")

    def _drop_db_tables(self):
        confirm = input("Are you sure you want to drop all created tables? (yes/no): ")
        if confirm.lower() == 'yes':
            if self.db_handler.drop_tables():
                print("Tables dropped successfully.")
            else:
                print("Failed to drop tables.")
        else:
            print("Table dropping cancelled.")


def main():
    config = configparser.ConfigParser()
    try:
        config.read('config.ini')
        db_config = dict(config['DATABASE'])
        api_base_url = config['API']['base_url']
    except Exception as e:
        logging.error(f"Error reading config.ini: {e}")
        return

    db_handler = MySQLHandler(db_config)
    api_client = APIClient(api_base_url)

    if not db_handler.create_tables():
        logging.error("Failed to create or verify database tables. Exiting.")
        db_handler.close()
        return

    cli_manager = CLIManager(api_client, db_handler)
    cli_manager.run()
    db_handler.close()

if __name__ == "__main__":
    main()


# === api_client.py ===
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
        """Fetch CSV, parse into DataFrame, apply filters."""
        try:
            logging.info(f"Attempting to fetch data from: {self.base_url}")
            response = requests.get(self.base_url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"API Request Error: {e}")
            return None

        try:
            df = pd.read_csv(StringIO(response.text))
            logging.info(f"Successfully fetched {len(df)} raw records from API.")
        except pd.errors.EmptyDataError:
            logging.warning("API returned empty data or invalid/empty CSV.")
            return None
        except Exception as e:
            logging.error(f"Failed to parse CSV into DataFrame: {e}")
            return None

        if 'location' not in df.columns or 'date' not in df.columns:
            logging.error("Fetched CSV missing required columns 'location' or 'date'.")
            return None

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

        if 'date' in df.columns:
            df = df.dropna(subset=['date'])

        return df.to_dict(orient='records')


# === data_transformer.py ===
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataTransformer:
    """Handles data cleaning and transformation."""

    def __init__(self):
        pass

    def clean_and_transform(self, raw_data, target_table_name):
        if raw_data is None or (isinstance(raw_data, list) and not raw_data):
            logging.warning("No raw data provided for transformation.")
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        original_rows = len(df)
        logging.info(f"Starting transformation for {original_rows} records for table '{target_table_name}'.")

        df = df.rename(columns={
            'date': 'report_date',
            'location': 'country_name',
            'total_cases': 'total_cases',
            'new_cases': 'new_cases',
            'total_deaths': 'total_deaths',
            'new_deaths': 'new_deaths',
            'total_vaccinations': 'total_vaccinations',
            'people_vaccinated': 'people_vaccinated',
            'people_fully_vaccinated': 'people_fully_vaccinated'
        })

        if 'report_date' not in df.columns or 'country_name' not in df.columns:
            logging.error("Missing 'date' or 'location' column in raw data. Cannot proceed with transformation.")
            return pd.DataFrame()

        df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')
        df = df.dropna(subset=['report_date'])
        logging.info(f"After date conversion and dropping NaNs: {len(df)} records.")

        daily_cases_cols = ['total_cases', 'new_cases', 'total_deaths', 'new_deaths']
        vaccination_cols = ['total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated']

        if target_table_name == 'daily_cases':
            numerical_cols = daily_cases_cols
        elif target_table_name == 'vaccination_data':
            numerical_cols = vaccination_cols
        else:
            numerical_cols = []

        for col in numerical_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            else:
                df[col] = 0
        logging.info(f"After numerical type conversion and fillna: {len(df)} records.")

        country_mapping = {
            'United States': 'United States',
            'United Kingdom': 'United Kingdom'
        }
        if 'country_name' in df.columns:
            df['country_name'] = df['country_name'].replace(country_mapping).fillna('Unknown')
        logging.info(f"After country name standardization: {len(df)} records.")

        if 'country_name' in df.columns and 'report_date' in df.columns:
            df = df.drop_duplicates(subset=['country_name', 'report_date'])
        logging.info(f"After dropping duplicates: {len(df)} records.")

        if target_table_name == 'daily_cases':
            final_cols = ['report_date', 'country_name'] + [col for col in daily_cases_cols if col in df.columns]
        elif target_table_name == 'vaccination_data':
            final_cols = ['report_date', 'country_name'] + [col for col in vaccination_cols if col in df.columns]
        else:
            logging.error(f"Unknown target table name: {target_table_name}. Cannot select columns.")
            return pd.DataFrame()

        df = df[final_cols]
        logging.info(f"Transformation complete. {len(df)} records ready for loading.")
        return df


# === mysql_handler.py ===
import mysql.connector
from mysql.connector import Error
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MySQLHandler:
    """Manages MySQL database connections and operations."""

    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.connect()

    def connect(self):
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            if self.conn.is_connected():
                db_info = self.conn.get_server_info()
                logging.info(f"Connected to MySQL database. Server version: {db_info}")
                return True
        except Error as e:
            logging.error(f"Error connecting to MySQL database: {e}")
            self.conn = None
            return False

    def create_tables(self):
        if not self.conn or not self.conn.is_connected():
            logging.error("Database connection not established. Cannot create tables.")
            return False
        try:
            cursor = self.conn.cursor()
            with open('sql/create_tables.sql', 'r') as f:
                sql_script = f.read()
            for statement in sql_script.split(';'):
                if statement.strip():
                    cursor.execute(statement)
            self.conn.commit()
            logging.info("Tables created/verified successfully.")
            return True
        except Error as e:
            logging.error(f"Error creating tables: {e}")
            self.conn.rollback()
            return False
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()

    def insert_data(self, table_name, data_list):
        if not self.conn or not self.conn.is_connected():
            logging.error("Database connection not established. Cannot insert data.")
            return 0
        if not data_list:
            logging.info(f"No data to insert into {table_name}.")
            return 0
        cursor = self.conn.cursor()
        inserted_count = 0
        try:
            select_sql = f"SELECT report_date, country_name FROM {table_name} WHERE report_date = %s AND country_name = %s"
            columns = data_list[0].keys()
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            new_records = []
            for record in data_list:
                cursor.execute(select_sql, (record['report_date'], record['country_name']))
                if not cursor.fetchone():
                    new_records.append(tuple(record.values()))
            if new_records:
                cursor.executemany(insert_sql, new_records)
                self.conn.commit()
                inserted_count = len(new_records)
                logging.info(f"Successfully inserted {inserted_count} new records into {table_name}.")
            else:
                logging.info(f"No new records to insert into {table_name}.")
        except Error as err:
            logging.error(f"Error inserting data into {table_name}: {err}")
            self.conn.rollback()
        finally:
            if cursor:
                cursor.close()
        return inserted_count

    def query_data(self, sql_query, params=None):
        if not self.conn or not self.conn.is_connected():
            logging.error("Database connection not established. Cannot query data.")
            return None
        cursor = self.conn.cursor(dictionary=True)
        try:
            cursor.execute(sql_query, params)
            return cursor.fetchall()
        except Error as e:
            logging.error(f"Error executing query: {e}")
            return None
        finally:
            if cursor:
                cursor.close()

    def list_tables(self):
        if not self.conn or not self.conn.is_connected():
            logging.error("Database connection not established. Cannot list tables.")
            return []
        cursor = self.conn.cursor()
        try:
            cursor.execute("SHOW TABLES")
            return [t[0] for t in cursor.fetchall()]
        except Error as e:
            logging.error(f"Error listing tables: {e}")
            return []
        finally:
            if cursor:
                cursor.close()

    def drop_tables(self):
        if not self.conn or not self.conn.is_connected():
            logging.error("Database connection not established. Cannot drop tables.")
            return False
        cursor = self.conn.cursor()
        try:
            tables_to_drop = ['daily_cases', 'vaccination_data']
            for table in tables_to_drop:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                logging.info(f"Dropped table: {table}")
            self.conn.commit()
            logging.info("All specified tables dropped successfully.")
            return True
        except Error as e:
            logging.error(f"Error dropping tables: {e}")
            self.conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()

    def close(self):
        if self.conn and self.conn.is_connected():
            self.conn.close()
            logging.info("MySQL connection closed.")
