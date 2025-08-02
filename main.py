import argparse
import logging
import configparser
from datetime import datetime, timedelta
from tabulate import tabulate # For pretty printing tables

from api_client import APIClient
from data_transformer import DataTransformer
from mysql_handler import MySQLHandler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CLIManager:
    """Manages the command-line interface."""

    def __init__(self, api_client, db_handler):
        self.api_client = api_client
        self.db_handler = db_handler
        self.parser = argparse.ArgumentParser(description="Global Healthcare Data ETL & Analysis CLI")
        self._setup_parser()

    def _setup_parser(self):
        subparsers = self.parser.add_subparsers(dest='command', help='Available commands')

        # ETL Command
        fetch_parser = subparsers.add_parser('fetch_data', help='Fetch, transform, and load healthcare data.')
        fetch_parser.add_argument('--country', type=str, help='Country to fetch data for (e.g., "India")', required=True)
        fetch_parser.add_argument('--start_date', type=str, help='Start date (YYYY-MM-DD)')
        fetch_parser.add_argument('--end_date', type=str, help='End date (YYYY-MM-DD)')

        # Query Commands
        query_parser = subparsers.add_parser('query_data', help='Query loaded data.')
        query_subparsers = query_parser.add_subparsers(dest='query_type', help='Types of queries')

        total_cases_parser = query_subparsers.add_parser('total_cases', help='Get total cases for a country.')
        total_cases_parser.add_argument('country', type=str, help='Country name.')

        daily_trends_parser = query_subparsers.add_parser('daily_trends', help='Get daily trends for a metric.')
        daily_trends_parser.add_argument('country', type=str, help='Country name.')
        daily_trends_parser.add_argument('metric', type=str, help='Metric (e.g., "new_cases", "new_deaths").')

        top_n_parser = query_subparsers.add_parser('top_n_countries_by_metric', help='Get top N countries by a metric.')
        top_n_parser.add_argument('n', type=int, help='Number of top countries.')
        top_n_parser.add_argument('metric', type=str, help='Metric (e.g., "total_cases", "total_vaccinations").')

        # DB Management Commands
        subparsers.add_parser('list_tables', help='List tables in the database.')
        subparsers.add_parser('drop_tables', help='Drop all created tables (USE WITH CAUTION).')

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
        # If dates are omitted, let fetch_data handle None (interpreted as unbounded)
        raw_data = self.api_client.fetch_data(country=country, start_date=start_date, end_date=end_date)


        if raw_data is None or not raw_data:
            logging.error("Failed to fetch raw data or no data available for the specified criteria.")
            return

        transformer = DataTransformer()

        # Process for daily_cases table
        logging.info("Transforming data for 'daily_cases' table...")
        transformed_cases_df = transformer.clean_and_transform(raw_data, 'daily_cases')
        if not transformed_cases_df.empty:
            self.db_handler.insert_data('daily_cases', transformed_cases_df.to_dict(orient='records'))
        else:
            logging.warning("No data to load into 'daily_cases' after transformation.")

        # Process for vaccination_data table
        logging.info("Transforming data for 'vaccination_data' table...")
        transformed_vacc_df = transformer.clean_and_transform(raw_data, 'vaccination_data')
        if not transformed_vacc_df.empty:
            self.db_handler.insert_data('vaccination_data', transformed_vacc_df.to_dict(orient='records'))
        else:
            logging.warning("No data to load into 'vaccination_data' after transformation.")

        logging.info("Data fetching and loading process completed.")

    def _execute_query(self, args):
        results = None
        headers = []

        if args.query_type == 'total_cases':
            sql_query = """
            SELECT country_name, SUM(total_cases) AS total_cases
            FROM daily_cases
            WHERE country_name = %s
            GROUP BY country_name;
            """
            results = self.db_handler.query_data(sql_query, (args.country,))
            headers = ["Country", "Total Cases"]
            if results:
                print(f"\nTotal COVID-19 Cases in {args.country}: {results[0]['total_cases']:,}")
            else:
                print(f"No data found for total cases in {args.country}.")

        elif args.query_type == 'daily_trends':
            if args.metric not in ['new_cases', 'new_deaths']:
                logging.error(f"Invalid metric for daily trends: {args.metric}. Choose 'new_cases' or 'new_deaths'.")
                return

            sql_query = f"""
            SELECT report_date, {args.metric}
            FROM daily_cases
            WHERE country_name = %s
            ORDER BY report_date ASC;
            """
            results = self.db_handler.query_data(sql_query, (args.country,))
            headers = ["Date", args.metric.replace('_', ' ').title()]
            if results:
                print(f"\nDaily {args.metric.replace('_', ' ').title()} Trends for {args.country}:")
                print(tabulate(results, headers=headers, tablefmt="grid"))
            else:
                print(f"No daily trends found for {args.country} for metric {args.metric}.")

        elif args.query_type == 'top_n_countries_by_metric':
            if args.metric not in ['total_cases', 'total_deaths', 'total_vaccinations']:
                logging.error(f"Invalid metric for top N countries: {args.metric}. Choose 'total_cases', 'total_deaths', or 'total_vaccinations'.")
                return

            table_name = 'daily_cases' if args.metric in ['total_cases', 'total_deaths'] else 'vaccination_data'
            
            # For total_cases/deaths, we need the latest cumulative value per country
            # For total_vaccinations, we need the latest cumulative value per country
            sql_query = f"""
            SELECT country_name, MAX({args.metric}) AS latest_metric_value
            FROM {table_name}
            GROUP BY country_name
            ORDER BY latest_metric_value DESC
            LIMIT %s;
            """
            results = self.db_handler.query_data(sql_query, (args.n,))
            headers = ["Rank", "Country", args.metric.replace('_', ' ').title()]
            
            if results:
                print(f"\nTop {args.n} Countries by {args.metric.replace('_', ' ').title()}:")
                formatted_results = []
                for i, row in enumerate(results):
                    formatted_results.append([i + 1, row['country_name'], f"{row['latest_metric_value']:,}"])
                print(tabulate(formatted_results, headers=headers, tablefmt="grid"))
            else:
                print(f"No data found for top {args.n} countries by {args.metric}.")
        else:
            logging.warning("Unknown query type.")

    def _list_db_tables(self):
        tables = self.db_handler.list_tables()
        if tables:
            print("\nTables in the database:")
            for table in tables:
                print(f"- {table}")
        else:
            print("No tables found in the database.")

    def _drop_db_tables(self):
        confirm = input("Are you sure you want to drop all created tables? This action cannot be undone. (yes/no): ")
        if confirm.lower() == 'yes':
            if self.db_handler.drop_tables():
                print("Tables dropped successfully.")
            else:
                print("Failed to drop tables.")
        else:
            print("Table dropping cancelled.")

def main():
    # Load database and API configuration
    config = configparser.ConfigParser()
    try:
        config.read('config.ini')
        db_config = dict(config['DATABASE'])
        api_base_url = config['API']['base_url']
    except KeyError as e:
        logging.error(f"Missing configuration section or key in config.ini: {e}")
        logging.error("Please ensure config.ini has [DATABASE] and [API] sections with required keys.")
        return
    except Exception as e:
        logging.error(f"Error reading config.ini: {e}")
        return

    # Initialize handlers
    db_handler = MySQLHandler(db_config)
    api_client = APIClient(api_base_url)

    # Ensure tables exist on startup
    if not db_handler.create_tables():
        logging.error("Failed to create or verify database tables. Exiting.")
        db_handler.close()
        return

    cli_manager = CLIManager(api_client, db_handler)
    cli_manager.run()

    db_handler.close()

if __name__ == "__main__":
    main()