import mysql.connector
from mysql.connector import Error
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MySQLHandler:
    """Manages MySQL database connections and operations."""

    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.connect()

    def connect(self):
        """Establishes a connection to the MySQL database."""
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            if self.conn.is_connected():
                logging.info(f"Connected to MySQL database")
                return True
        except Error as e:
            logging.error(f"Error connecting to MySQL database: {e}")
            self.conn = None
            return False

    def create_tables(self):
        """Creates necessary tables in the database from SQL script."""
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
        """
        Inserts a list of dictionaries/records into a specified table.
        
        """
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
                else:
                    logging.debug(f"Skipping existing record: {record['country_name']} on {record['report_date']}")

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
        """Executes a SELECT query and returns results."""
        if not self.conn or not self.conn.is_connected():
            logging.error("Database connection not established.")
            return None
        cursor = self.conn.cursor(dictionary=True) 
        try:
            cursor.execute(sql_query, params)
            results = cursor.fetchall()
            return results
        except Error as e:
            logging.error(f"Error executing query: {e}")
            return None
        finally:
            if cursor:
                cursor.close()

    def list_tables(self):
        """Lists all tables in the connected database."""
        if not self.conn or not self.conn.is_connected():
            logging.error("Database connection not established.")
            return []
        cursor = self.conn.cursor()
        try:
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            return tables
        except Error as e:
            logging.error(f"Error listing tables: {e}")
            return []
        finally:
            if cursor:
                cursor.close()

    def drop_tables(self):
        """Drops all created tables ."""
        if not self.conn or not self.conn.is_connected():
            logging.error("Database connection not established.")
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
        """Closes the database connection."""
        if self.conn and self.conn.is_connected():
            self.conn.close()
            logging.info("connection closed.")


