Healthcare Data CLI ETL Project
A command-line tool for extracting, transforming, and loading (ETL) global COVID-19 case and vaccination data from a public API, storing and analyzing it using MySQL.

Table of Contents
Installation and Setup

Public Data Source

CLI Command Usage

Database Schema

ETL Process Overview

Known Limitations

Future Improvements

Installation and Setup
Prerequisites
Python 3.7+

MySQL Server (5.7 or higher recommended)

Git (optional, for cloning the repo)

Step 1: Clone the Project
text
git clone https://your-repo-url.git
cd healthcare_etl_cli
Step 2: Install Dependencies
text
pip install -r requirements.txt
Step 3: Configure Database Connection
Edit config.ini:

text
[mysql]
host = localhost
user = your_mysql_username
password = your_mysql_password
database = covid_healthcare
Note:
Create the database in MySQL if it does not exist:

sql
CREATE DATABASE covid_healthcare;
Step 4: Initialize Database Tables
The tables are created automatically when you use CLI commands, but you can also do it manually:

text
mysql -u root -p covid_healthcare < sql/create_tables.sql
Public Data Source
This project uses open global COVID-19 data from [Our World in Data (OWID)]:

API Endpoint:
https://github.com/owid/covid-19-data/raw/master/public/data/owid-covid-data.csv

Data fields include:
Country, Date, Total Cases, New Cases, Total Deaths, New Deaths, Total Vaccinations, People Vaccinated, People Fully Vaccinated, and more.

Update frequency:
Usually daily.

CLI Command Usage
All commands use python main.py as an entry point.

1. Fetch Data and Load to Database
Fetch case and/or vaccination data for a country and time range:

text
python main.py fetch_data --country "India" --start_date "2023-01-01" --end_date "2023-01-31" --data_type all
--country: Name of the country.

--start_date, --end_date: Dates in YYYY-MM-DD.

--data_type: cases, vaccinations, or all (default cases).

2. Query Analytics
a. Get Latest Total Cases:

text
python main.py query_data total_cases --country "India"
b. Show Daily Trends:

text
python main.py query_data daily_trends --country "USA" --metric "new_cases"
c. Top N Countries By Metric:

text
python main.py query_data top_n_countries_by_metric --n 5 --metric "total_vaccinations"
3. List Tables
text
python main.py list_tables
4. Drop All Data Tables
text
python main.py drop_tables
Database Schema
Table: daily_cases

Column	Type	Description
id	INT (PK)	Row ID (auto-increment)
report_date	DATE	Date of record
country_name	VARCHAR(255)	Country
total_cases	BIGINT	Cumulative cases
new_cases	INT	New cases reported that day
total_deaths	BIGINT	Cumulative deaths
new_deaths	INT	New deaths that day
etl_timestamp	TIMESTAMP	When record was loaded
Table: vaccination_data

Column	Type	Description
id	INT (PK)	Row ID (auto-increment)
report_date	DATE	Date of record
country_name	VARCHAR(255)	Country
total_vaccinations	BIGINT	Cumulative vaccinations
people_vaccinated	BIGINT	At least 1 dose
people_fully_vaccinated	BIGINT	Fully vaccinated
etl_timestamp	TIMESTAMP	When record was loaded
See sql/create_tables.sql for details.

ETL Process Overview
Extract:
Pulls the latest CSV data from OWID’s public API using HTTP.

Transform:

Filters data for requested country and date range.

Converts data types and handles missing/null values.

Normalizes column names and formats for easy analysis.

Load:

Writes processed records into MySQL tables: daily_cases and vaccination_data, using upserts to avoid duplicates.

Analyze:

Enables queries for trends and summary analytics via the CLI.

Known Limitations
No incremental update:
Every fetch reloads the full specified date/country range; incremental/delta updates aren’t yet optimized.

API reliability:
Relies on the continued availability and format stability of the OWID CSV.

Static country naming:
Typos or alternate country naming in user input may lead to empty results.

Minimal error handling:
Command errors or data fetch failures aren’t exhaustively handled.

No Docker support:
MySQL and the app must be installed/configured independently.

Future Improvements
Incremental Data Loading:
Only update new or changed data from API.

Input Validation:
Validate country names and date formats interactively.

Advanced Analytics:
Add more CLI queries and visualizations.

Robust Logging/Error Handling:
More informative CLI and logs for debugging and monitoring.

Dockerize Project:
Bundled MySQL + app for easy deployment.

Automated Testing and CI:
Basic unit/integration tests and CI setup.