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
    connection_string = os.getenv("CONNECTION_STRING")
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
        print(f"Last load timestamp read from file: {last_load_timestamp}")
    
    # Check the contents of the DataFrame before filtering
    print(f"Total rows in the DataFrame before filtering: {len(df_cleaned)}")
    
    # Filter the DataFrame to include only new or updated records for incremental load
    if last_load_timestamp:
        last_load_timestamp = pd.to_datetime(last_load_timestamp)
        df_cleaned = df_cleaned[df_cleaned['date'] > last_load_timestamp]
        print(f"Filtered DataFrame with records after {last_load_timestamp}: {len(df_cleaned)} rows remaining.")
    
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
                volume BIGINT,
                CONSTRAINT unique_price UNIQUE (Stock_ID, date)
            );
        """)
        connection.commit()
        cursor.close()
        print("Tables created successfully.")
    
    # Create the tables
    create_tables(connection)
    
    # Insert data into Stock_Table without Stock_ID (it will auto-generate)
    def insert_stock_table_data(connection, stock_table_df):
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO Stock_Table (symbol)
            VALUES (%s)
            ON CONFLICT (symbol) DO NOTHING;
        """
        for _, row in stock_table_df.iterrows():
            cursor.execute(insert_query, (row['symbol'],))
        connection.commit()
        cursor.close()
        print("Data inserted into Stock_Table.")
    
    # Retrieve Stock_ID based on symbol after insertion
    def get_stock_id_for_symbol(connection, symbol):
        cursor = connection.cursor()
        query = "SELECT Stock_ID FROM Stock_Table WHERE symbol = %s"
        cursor.execute(query, (symbol,))
        stock_id = cursor.fetchone()[0]
        cursor.close()
        return stock_id
    
    # Insert data into Stock_Price_Table
    def insert_stock_price_table_data(connection, stock_price_table_df):
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO Stock_Price_Table (Stock_ID, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (Stock_ID, date) DO NOTHING;
        """
        for _, row in stock_price_table_df.iterrows():
            stock_id = get_stock_id_for_symbol(connection, row['symbol'])
            cursor.execute(insert_query, (
                stock_id,
                row['date'],
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume']
            ))
        connection.commit()
        cursor.close()
        print("Data inserted into Stock_Price_Table.")
    
    # Update the last_load_timestamp.txt file after data insertion
    def update_last_load_timestamp(latest_timestamp):
        with open("last_load_timestamp.txt", "w") as file:
            file.write(str(latest_timestamp))
        print(f"Last load timestamp updated to: {latest_timestamp}")
    
    # Prepare data for insertion into Stock_Table
    stock_table_df = df_cleaned[['symbol']].drop_duplicates()
    
    # Insert data into Stock_Table
    insert_stock_table_data(connection, stock_table_df)
    
    # Prepare Stock_Price_Table DataFrame
    stock_price_table_df = df_cleaned[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
    
    # Insert data into Stock_Price_Table if there are rows left after filtering
    if not df_cleaned.empty:
        print(f"Preparing to insert {len(stock_price_table_df)} rows into Stock_Price_Table.")
        insert_stock_price_table_data(connection, stock_price_table_df)
        
        # Update the timestamp only if new data was inserted
        latest_timestamp = df_cleaned['date'].max()
        update_last_load_timestamp(latest_timestamp)
    else:
        print("No new data to insert, timestamp not updated.")

# Call the function to load data to PostgreSQL
load_data_to_postgres()
