import requests
import pandas as pd
import os
from io import StringIO
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

def extract_and_upload(api_key, symbols, blob_name, function='TIME_SERIES_DAILY', outputsize='full'):
    # Dictionary to store the DataFrame for each stock
    stocks_df = {}

    for symbol in symbols:
        # Construct the API URL for the current stock
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&outputsize={outputsize}&apikey={api_key}'
        
        try:
            # Send the GET request to the API
            r = requests.get(url)
            r.raise_for_status()  # Raise an error for bad responses
            data = r.json()

            # Extract the time series data for the stock
            time_series = data.get('Time Series (Daily)', {})
            
            if not time_series:
                print(f"Failed to retrieve data for {symbol}: {data.get('Error Message', 'No data available')}")
                continue  # Skip to the next symbol if data is missing

            # Convert the data into a pandas DataFrame
            df = pd.DataFrame.from_dict(time_series, orient='index')

            # Rename columns for easier access
            df.columns = ['open', 'high', 'low', 'close', 'volume']
            
            # Convert the index (dates) to datetime format
            df.index = pd.to_datetime(df.index)
            
            # Ensure the values are numeric
            df = df.apply(pd.to_numeric)
            
            # Sort the DataFrame by date (index)
            df = df.sort_index()

            # Store the DataFrame in the dictionary with the symbol as the key
            stocks_df[symbol] = df

        except requests.RequestException as e:
            print(f"Error fetching data for {symbol}: {e}")

    # List to store individual DataFrames with a new 'symbol' column
    df_list = []

    # Iterate over the dictionary
    for symbol, df in stocks_df.items():
        # Add a 'symbol' column to each DataFrame
        df['symbol'] = symbol
        df_list.append(df)

    # Concatenate all the DataFrames into one DataFrame
    combined_df = pd.concat(df_list)

    # Reset the index to have a continuous index
    combined_df.reset_index(inplace=True)

    # Rename the index to 'date'
    combined_df.rename(columns={'index': 'date'}, inplace=True)

    # Upload the combined DataFrame to Azure Blob Storage
    upload_to_blob_storage(combined_df, blob_name)

def upload_to_blob_storage(df, blob_name):
    # Retrieve connection details from environment variables
    connect_str = os.getenv('CONNECTION_STRING')
    container_name = os.getenv('CONTAINER_NAME')

    # Create BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Get the container client
    container_client = blob_service_client.get_container_client(container_name)

    # Create a blob client
    blob_client = container_client.get_blob_client(blob_name)

    try:
        # Convert DataFrame to CSV format in memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload the CSV data to Azure Blob Storage
        blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        print(f"File '{blob_name}' uploaded to blob storage successfully.")
    except Exception as ex:
        print(f"An error occurred: {ex}")

# callthe function
if __name__ == "__main__":
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')  ##'8GSEZH7YO4E598CK'  Store your API key in .env
    symbols = ['AAPL', 'MSFT', 'TSLA', 'IBM', 'AMZN']
    
    # Name of the blob you want to create
    blob_name = "rawdata/raw_data.csv"  
    # Extract data from Alpha Vantage API and upload to Azure Blob Storage
    extract_and_upload(api_key, symbols, blob_name)
