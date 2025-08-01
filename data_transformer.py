import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataTransformer:
    """Handles data cleaning and transformation."""

    def __init__(self):
        pass

    def clean_and_transform(self, raw_data, target_table_name):
        """
        Cleans and transforms raw data into a standardized Pandas DataFrame
        ready for loading into MySQL, based on the target table schema.
        """
        if not raw_data:
            logging.warning("No raw data provided for transformation.")
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)
        original_rows = len(df)
        logging.info(f"Starting transformation for {original_rows} records for table '{target_table_name}'.")

        # Rename columns to match MySQL table schema
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

        # Ensure essential columns exist
        if 'report_date' not in df.columns or 'country_name' not in df.columns:
            logging.error("Missing 'date' or 'location' column in raw data. Cannot proceed with transformation.")
            return pd.DataFrame()

        # 1. Convert 'report_date' column to datetime objects
        df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')
        df = df.dropna(subset=['report_date']) # Drop rows where date conversion failed
        logging.info(f"After date conversion and dropping NaNs: {len(df)} records.")

        # 2. Convert numerical columns to appropriate types, handle non-numeric values
        # Define columns for daily_cases table
        daily_cases_cols = ['total_cases', 'new_cases', 'total_deaths', 'new_deaths']
        # Define columns for vaccination_data table
        vaccination_cols = ['total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated']

        if target_table_name == 'daily_cases':
            numerical_cols = daily_cases_cols
        elif target_table_name == 'vaccination_data':
            numerical_cols = vaccination_cols
        else:
            numerical_cols = [] # No specific numerical columns for other tables

        for col in numerical_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            else:
                df[col] = 0 # Add column with default 0 if not present in raw data
        logging.info(f"After numerical type conversion and fillna: {len(df)} records.")

        # 3. Standardize country names (if necessary, e.g., 'United States' to 'USA')
        # The OWID dataset generally uses consistent country names, but this is a good place for custom mapping
        country_mapping = {
            'United States': 'United States', # Example: no change needed for OWID
            'United Kingdom': 'United Kingdom'
        }
        if 'country_name' in df.columns:
            df['country_name'] = df['country_name'].replace(country_mapping).fillna('Unknown')
        logging.info(f"After country name standardization: {len(df)} records.")

        # 4. Handle duplicates based on (country_name, report_date)
        if 'country_name' in df.columns and 'report_date' in df.columns:
            df = df.drop_duplicates(subset=['country_name', 'report_date'])
        logging.info(f"After dropping duplicates: {len(df)} records.")

        # Select only columns relevant to the target table
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

