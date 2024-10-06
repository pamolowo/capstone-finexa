# Setting up widgets and user
dbutils.widgets.text("user", "default_user")
user = dbutils.widgets.get("user")
print(f"Running job for user: {user}")

# Unmount the existing directory
dbutils.fs.unmount("/mnt/alphafinexacontainer")
storage_account_name = "alphafinexa"
storage_account_key=os.getenv("STORAGE_ACCOUNT_KEY")
mount_point = "/mnt/alphafinexacontainer"  # This is where the blob will be mounted in DBFS

# Check if the directory is already mounted
mounts = [mnt.mountPoint for mnt in dbutils.fs.mounts()]

if mount_point not in mounts:
    dbutils.fs.mount(
      source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
      mount_point = mount_point,
      extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
    )
else:
    print(f"{mount_point} is already mounted.")


import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import os
#To access the data from the blob storage
# Path to the file in Blob Storage
file_path = f"/mnt/alphafinexacontainer/rawdata/raw_data.csv"

# Read the file into a Spark DataFrame
df = spark.read.format("csv").option("header", "true").load(file_path)

# Show the first few rows of the DataFrame
df.show()
df.show(20) #Viewing the data
display(df)
df.printSchema()
#Finding the Missing Data

for column in df.columns:
    print(column, 'Null:', df.filter(df[column].isNull()).count())
  # Data Cleaning
df_cleaned = df.select(
        col("date"),
        col("open"),
        col("high"),
        col("low"),
        col("close"),
        col("symbol"),  # Include the symbol column
        when(col("volume").isNull(), 0).otherwise(col("volume")).alias("volume")  # Replace nulls with 0 in volume
    ).dropDuplicates()

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key)
from datetime import datetime
# or use uuid if preferred
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Define your file path
output_csv_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/cleaned_data/csv_output_{run_id}.csv"

# Save the DataFrame to Azure Blob Storage as a CSV
df_cleaned.write.mode("overwrite").option("header", "true").csv(output_csv_path)




# Push the path to XCom
dbutils.notebook.exit(output_csv_path)
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from datetime import datetime

# Azure Blob Storage details
STORAGE_ACCOUNT_NAME = "alphafinexa"
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
RAW_DATA_BLOB_PATH = "rawdata/raw_data.csv"

# Generate a unique run ID for cleaned data output
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
CLEANED_DATA_DIR = "cleaned_data"
CLEANED_DATA_BLOB_PATH = os.path.join(CLEANED_DATA_DIR, f"csv_output_{run_id}.csv")

def download_blob(blob_service_client, container_name, blob_path, local_file_path):
    """Download a blob to a local file."""
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    with open(local_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

def upload_blob(blob_service_client, container_name, local_file_path, blob_path):
    """Upload a local file to a blob."""
    cleaned_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    with open(local_file_path, "rb") as data:
        cleaned_blob_client.upload_blob(data, overwrite=True)

def main():
    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient(account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net", 
                                             credential=STORAGE_ACCOUNT_KEY)

    # Download raw CSV data from Azure Blob Storage
    local_raw_data_file = "raw_data.csv"
    download_blob(blob_service_client, CONTAINER_NAME, RAW_DATA_BLOB_PATH, local_raw_data_file)

    # Load the data into a pandas DataFrame
    df = pd.read_csv(local_raw_data_file)

    # Data Cleaning
    df_cleaned = df[['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']].copy()
    df_cleaned['volume'] = df_cleaned['volume'].fillna(0)
    df_cleaned.drop_duplicates(inplace=True)

    # Create cleaned_data directory if it doesn't exist
    os.makedirs(CLEANED_DATA_DIR, exist_ok=True)

    # Save the cleaned DataFrame to a CSV file locally
    df_cleaned.to_csv(CLEANED_DATA_BLOB_PATH, index=False)

    # Upload the cleaned CSV back to Azure Blob Storage
    upload_blob(blob_service_client, CONTAINER_NAME, CLEANED_DATA_BLOB_PATH, CLEANED_DATA_BLOB_PATH)

    print(f"Cleaned data uploaded to: {CLEANED_DATA_BLOB_PATH}")

if __name__ == "__main__":
    main()
