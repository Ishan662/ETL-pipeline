import pandas as pd
import numpy as np
import sqlite3
import datetime
from bs4 import BeautifulSoup
import requests

RESOURCE_LINK = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
TARGET_FILE = "largest_banks_data.csv"
LOG_FILE = "code_log.txt"


def log_process(message):
    timestamp_format = '%Y-%m-%d-%H-%M-%S'
    now = datetime.datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(LOG_FILE, "a") as f:
        f.write(timestamp + ',' + message + '\n')


def extract(csv_url, html_url):
    # Directly read the CSV file using Pandas
    df = pd.read_csv(csv_url)
    print("Extracted DataFrame:")
    print(df.head())

    # Fetch and print the HTML content using BeautifulSoup
    response = requests.get(html_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    print("HTML Content:")
    print(soup.prettify())

    return df, soup


def transform(df, exchange_rates):
    print("DataFrame Columns:", df.columns)  # Print columns to inspect them
    if 'Rate' not in df.columns:
        raise KeyError("Expected column 'Rate' not found in the DataFrame.")

    df.dropna(inplace=True)
    df['Rate'] = df['Rate'].astype(float)

    df['MC_GBP_Billion'] = df['Rate'] * exchange_rates['GBP']
    df['MC_EUR_Billion'] = df['Rate'] * exchange_rates['EUR']
    df['MC_INR_Billion'] = df['Rate'] * exchange_rates['INR']

    log_process("Data transformed")
    return df


def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)
    log_process(f"Data loaded to CSV at {output_path}")


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_process(f"Data loaded to database table {table_name}")


def run_query(query_statement, sql_connection):
    result = pd.read_sql_query(query_statement, sql_connection)
    log_process(f"Query run: {query_statement}")
    return result


def get_exchange_rates(resource_link):
    exchange_rates_df = pd.read_csv(resource_link)
    exchange_rates = {
        'GBP': exchange_rates_df.loc[exchange_rates_df['Currency'] == 'GBP', 'Rate'].values[0],
        'EUR': exchange_rates_df.loc[exchange_rates_df['Currency'] == 'EUR', 'Rate'].values[0],
        'INR': exchange_rates_df.loc[exchange_rates_df['Currency'] == 'INR', 'Rate'].values[0]
    }
    return exchange_rates


# URLs for extracting data
csv_url = RESOURCE_LINK
html_url = "https://example.com/page-with-table"  # Replace with actual HTML URL

# Extract CSV and HTML data
df, soup = extract(csv_url, html_url)

# Get exchange rates and transform data
exchange_rates = get_exchange_rates(RESOURCE_LINK)
df = transform(df, exchange_rates)

# Load data to CSV and database
load_to_csv(df, TARGET_FILE)

conn = sqlite3.connect('Banks.db')
load_to_db(df, conn, 'largest_banks')

# Run a sample query and print the result
query = "SELECT * FROM largest_banks"
result = run_query(query, conn)
print(result)

conn.close()
