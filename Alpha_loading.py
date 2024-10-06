import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io
import psycopg2
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

def load_data_to_postgres():
    # Initialize the BlobServiceClient with your connection string from environment variable
    connection_string =os.getenv("CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Define your container and directory paths
    container_name = os.getenv("CONTAINER_NAME")
    cleaned_data_directory = "cleaned_data/"

    # Create a client for the container
    container_client = blob_service_client.get_container_client(container_name)

    # List all files in the cleaned_data directory
    print("Listing files in the cleaned_data directory:")
    blobs_list = container_client.list_blobs(name_starts_with=cleaned_data_directory)

    csv_files = []

    # Collect all CSV file names
    for blob in blobs_list:
        print(blob.name)  # Print each blob's name
        if blob.name.endswith('.csv'):  # Check if the blob is a CSV file
            csv_files.append(blob.name)

    # Check if any CSV files were found
    if not csv_files:
        raise ValueError("Error: No CSV files found in the cleaned_data directory.")
    
    # Sort the files by timestamp or name to get the latest one
    latest_csv_file = max(csv_files, key=lambda x: x.split('/')[-1])  # Get the latest file by name

    # Create a BlobClient for the latest CSV file
    blob_client = container_client.get_blob_client(latest_csv_file)

    # Download the CSV file into memory
    try:
        print(f"Downloading the latest CSV file: {latest_csv_file}")
        csv_data = blob_client.download_blob().readall()  # Read the content of the blob
        # Load the cleaned data into a DataFrame from the CSV data in memory
        df_cleaned = pd.read_csv(io.BytesIO(csv_data))  # Use BytesIO to read from memory
        print("DataFrame loaded from the CSV data in memory.")

        # Ensure that the Date column is properly formatted as a date
        df_cleaned['date'] = pd.to_datetime(df_cleaned['date'])
        print("Date column formatted successfully.")

    except Exception as e:
        raise RuntimeError(f"An error occurred while loading the CSV file: {e}")

    # Load the last load timestamp
    last_load_timestamp = None
    timestamp_file = "last_load_timestamp.txt"
    if os.path.exists(timestamp_file):
        with open(timestamp_file, "r") as file:
            last_load_timestamp = file.read().strip()

    # Filter the DataFrame to include only new or updated records
    if last_load_timestamp:
        last_load_timestamp = pd.to_datetime(last_load_timestamp)
        df_cleaned = df_cleaned[df_cleaned['date'] > last_load_timestamp]

    # Define the connection details for PostgreSQL from environment variables
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),  # or the appropriate IP address
        port="5432",
        database="finexa",
        user="postgres",
        password=os.getenv("POSTGRES_PASSWORD")
    )

    # Function to create tables
    def create_tables(connection):
        cursor = connection.cursor()

        # Create Stock_Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Stock_Table (
                Stock_ID SERIAL PRIMARY KEY,
                symbol VARCHAR(255) NOT NULL UNIQUE
            );
        """)

        # Create Stock_Price_Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Stock_Price_Table (
                ID SERIAL PRIMARY KEY,
                Stock_ID INT REFERENCES Stock_Table(Stock_ID),
                date DATE NOT NULL,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume BIGINT
            );
        """)

        connection.commit()
        cursor.close()
        print("Tables created successfully.")

    # Create the tables
    create_tables(connection)

    # Generate unique Stock_IDs for each symbol
    df_cleaned['Stock_ID'] = df_cleaned['symbol'].factorize()[0] + 1  # Start IDs from 1

    # Select unique stock symbols and their corresponding Stock_IDs for Stock_Table
    stock_table_df = df_cleaned[['Stock_ID', 'symbol']].drop_duplicates()

    # Prepare Stock_Price_Table DataFrame
    stock_price_table_df = df_cleaned[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
    stock_price_table_df['Stock_ID'] = df_cleaned['Stock_ID']

    # Insert data into Stock_Table
    def insert_stock_table_data(connection, stock_table_df):
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO Stock_Table (Stock_ID, symbol)
            VALUES (%s, %s)
            ON CONFLICT (Stock_ID) DO NOTHING;  
        """
        
        for _, row in stock_table_df.iterrows():
            cursor.execute(insert_query, (row['Stock_ID'], row['symbol']))
        
        connection.commit()
        cursor.close()

    # Insert data into Stock_Price_Table
    def insert_stock_price_table_data(connection, stock_price_table_df):
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO Stock_Price_Table (Stock_ID, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ID) DO NOTHING;  
        """
        
        for _, row in stock_price_table_df.iterrows():
            cursor.execute(insert_query, (
                row['Stock_ID'],
                row['date'],
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume']
            ))
        
        connection.commit()
        cursor.close()

    # Load the data into PostgreSQL
    insert_stock_table_data(connection, stock_table_df)
    insert_stock_price_table_data(connection, stock_price_table_df)

    # Update and save the last load timestamp
    if not df_cleaned.empty:
        last_load_timestamp = df_cleaned['date'].max()
        with open(timestamp_file, "w") as file:
            file.write(str(last_load_timestamp))
        print(f"Last load timestamp updated to {last_load_timestamp}")

    # Close the connection
    connection.close()

    print("Data loaded successfully into PostgreSQL.")

# Call the function to execute it
if __name__ == "__main__":
    load_data_to_postgres()
