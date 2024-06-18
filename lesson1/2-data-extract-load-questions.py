# Extract: Process to pull data from Source system
# Load: Process to write data to a destination system

# Common upstream & downstream systems
# OLTP Databases: Postgres, MySQL, sqlite3, etc
# OLAP Databases: Snowflake, BigQuery, Clickhouse, DuckDB, etc
# Cloud data storage: AWS S3, GCP Cloud Store, Minio, etc
# Queue systems: Kafka, Redpanda, etc
# API
# Local disk: csv, excel, json, xml files
# SFTP\FTP server

# Databases: When reading or writing to a database we use a database driver. Database drivers are libraries that we can use to read or write to a database.
# Question: How do you read data from a sqlite3 database and write to a DuckDB database?
# Hint: Look at importing the database libraries for sqlite3 and duckdb and create connections to talk to the respective databases

# Fetch data from the SQLite Customer table
import sqlite3

database_connection=sqlite3.connect(database='hyderabed')
curr_object = database_connection.cursor()

curr_object.execute('select * from table')
data_from_sqlite=curr_object.fetchall()

database_connection.close()

for row in data_from_sqlite:
    print(row)

import duckdb

connection_to_db=duckdb.connect(database='')

curr_objecct=connection_to_db.cursor()

curr_objecct.execute('CREATE TABLE IF NOT EXISTS duckdb_table (col1 INT, col2 TEXT)')

data_for_duckdb = [(1, 'foo'), (2, 'bar'), (3, 'baz')]
cursor_duckdb.executemany('INSERT INTO duckdb_table VALUES (?, ?)', data_for_duckdb)






# Insert data into the DuckDB Customer table

# Hint: Look for Commit and close the connections
# Commit tells the DB connection to send the data to the database and commit it, if you don't commit the data will not be inserted

# We should close the connection, as DB connections are expensive

# Cloud storage
# Question: How do you read data from the S3 location given below and write the data to a DuckDB database?
# Data source: https://docs.opendata.aws/noaa-ghcn-pds/readme.html station data at path "csv.gz/by_station/ASN00002022.csv.gz"
# Hint: Use boto3 client with UNSIGNED config to access the S3 bucket
# Hint: The data will be zipped you have to unzip it and decode it to utf-8

# AWS S3 bucket and file details
bucket_name = "noaa-ghcn-pds"
file_key = "csv.gz/by_station/ASN00002022.csv.gz"
# Create a boto3 client with anonymous access



# Download the CSV file from S3
# Decompress the gzip data
# Read the CSV file using csv.reader
# Connect to the DuckDB database (assume WeatherData table exists)

# Insert data into the DuckDB WeatherData table

# API
# Question: How do you read data from the CoinCap API given below and write the data to a DuckDB database?
# URL: "https://api.coincap.io/v2/exchanges"
# Hint: use requests library


# Define the API endpoint
url = "https://api.coincap.io/v2/exchanges"

# Fetch data from the CoinCap API
# Connect to the DuckDB database

# Insert data into the DuckDB Exchanges table
# Prepare data for insertion
# Hint: Ensure that the data types of the data to be inserted is compatible with DuckDBs data column types in ./setup_db.py
import requests

# Define the API endpoint
url = "https://api.coincap.io/v2/exchanges"

# Send a GET request to the API endpoint
response = requests.get(url)

# Parse the JSON response
data = response.json()

# Extract the relevant data
exchanges = data['data']


import duckdb

# Connect to DuckDB database (creates the file if it doesn't exist)
conn = duckdb.connect(database='crypto_data.duckdb')

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Example: Create the exchanges table (if it doesn't already exist)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS exchanges (
        exchangeId TEXT,
        name TEXT,
        rank INTEGER,
        percentTotalVolume REAL,
        volumeUsd REAL,
        tradingPairs INTEGER,
        socket TEXT,
        exchangeUrl TEXT,
        updated INTEGER
    )
''')

# Insert data into DuckDB exchanges table
for exchange in exchanges:
    cursor.execute('''
        INSERT INTO exchanges VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        exchange['exchangeId'],
        exchange['name'],
        int(exchange['rank']) if exchange['rank'] else None,
        float(exchange['percentTotalVolume']) if exchange['percentTotalVolume'] else None,
        float(exchange['volumeUsd']) if exchange['volumeUsd'] else None,
        int(exchange['tradingPairs']) if exchange['tradingPairs'] else None,
        exchange['socket'],
        exchange['exchangeUrl'],
        int(exchange['updated']) if exchange['updated'] else None
    ))

# Commit changes (if necessary)
conn.commit()

# Close the connection
conn.close()



# Local disk
# Question: How do you read a CSV file from local disk and write it to a database?
# Look up open function with csvreader for python

import csv
import duckdb

# Define the path to your CSV file
csv_file_path = 'your_local_file.csv'

# Connect to the DuckDB database (creates the file if it doesn't exist)
conn = duckdb.connect(database='my_database.duckdb')

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Example: Create a table in the database (if it doesn't already exist)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS my_table (
        column1 TEXT,
        column2 INTEGER,
        column3 REAL
    )
''')

# Read the CSV file and insert data into the database
with open(csv_file_path, mode='r') as file:
    csv_reader = csv.reader(file)
    # Skip the header row if your CSV file has one
    header = next(csv_reader)
    
    # Insert data into the table
    for row in csv_reader:
        cursor.execute('''
            INSERT INTO my_table (column1, column2, column3) VALUES (?, ?, ?)
        ''', row)

# Commit changes (if necessary)
conn.commit()

# Close the connection
conn.close()

print("Data successfully inserted into the database.")

# Web scraping
# Questions: Use beatiful soup to scrape the below website and print all the links in that website
# URL of the website to scrape
import requests
from bs4 import BeautifulSoup

# URL of the website to scrape
url = 'https://example.com'

# Send a GET request to the website
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the HTML content of the website
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all the <a> tags (which define hyperlinks)
    links = soup.find_all('a')
    
    # Print all the links
    for link in links:
        href = link.get('href')
        if href:
            print(href)
else:
    print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
