###
***Finexa Capital Financial Data Pipeline - Capstone Project***
This project demonstrates the creation of an automated financial data pipeline for Finexa Capital, a fictional financial institution specializing in investment management, market research, and financial advisory. This project addresses real-world challenges faced by financial institutions, such as the need for real-time data insights, incremental data loading, and secure data management.
###
***Overview***
The project involves automating the entire ETL process using Apache Airflow, from collecting financial data via an API, to transforming and cleaning the data using Azure Databricks, and finally loading it into a PostgreSQL database. Security measures have been incorporated using personal tokens, API keys, Azure Entra ID, and connection strings. Logging and error tracking are implemented throughout the process.
***

###
***Key Features:***
*Automated extraction of financial data from the Alpha Vantage API
*Incremental data loading for efficiency and data freshness
*Data transformation using Azure Databricks
*Secure data handling with personal tokens, API keys, connection strings, and Azure Entra ID
*Data validation and error logging at every stage
*Scalable cloud infrastructure using Azure Blob Storage for data storage and PostgreSQL for data persistence
***Prerequisites***
*Ubuntu Virtual Environment: Ubuntu 20.04 or higher (installation can be done locally or on any cloud instance).
*Azure Subscription: Ensure you have an active Azure account for setting up resources.
*API Key for Alpha Vantage: Required to fetch stock data.
*PostgreSQL Server: Either locally hosted or on a cloud provider.

###
***Step-by-Step Setup Guide***
1. Set Up the Ubuntu Virtual Environment
First, create a virtual environment on your Ubuntu machine:

###
sudo apt update && sudo apt upgrade
sudo apt install python3-venv
python3 -m venv finexa-venv
source finexa-venv/bin/activate
###


###
***2. Install Required Dependencies***
Inside your virtual environment, install the following Python dependencies for the project:


pip install apache-airflow
pip install python-dotenv
pip install azure-storage-blob
pip install pyspark
pip install pandas
pip install psycopg2  # PostgreSQL connector
Other dependencies may be installed based on the environment, for instance, requests for API calls.

###
***3. Install and Configure Apache Airflow***
To orchestrate tasks with Airflow:

Initialize Airflow:


airflow db init
Create an Airflow user:


airflow users create --username admin --password admin --firstname FIRST --lastname LAST --role Admin --email admin@example.com
Start the Airflow web server and scheduler:



airflow webserver --port 8080
airflow scheduler


###
***4. Set Up Azure Resources****
4.1 Create an Azure Resource Group

az group create --name FinexaResourceGroup --location eastus

###
4.2 Create an Azure Storage Account

az storage account create --name finexastorage --resource-group FinexaResourceGroup --location eastus --sku Standard_LRS

###
4.3 Create a Blob Storage Container

az storage container create --name finexablob --account-name finexastorage --resource-group FinexaResourceGroup


###
4.4 Set Up Azure Databricks Workspace
Create a Databricks workspace:


az databricks workspace create --resource-group FinexaResourceGroup --name finexadbworkspace --location eastus
Create Databricks jobs using the UI, which will handle the transformation of the extracted data.


###
4.5 Set Up PostgreSQL
Create a PostgreSQL server instance:

###
az postgres server create --resource-group FinexaResourceGroup --name finexapostgres --location eastus --admin-user adminuser --admin-password SecurePassword123 --sku-name B_Gen5_2
Create the required databases and tables within your PostgreSQL instance.


###
***5. Configure Security***
5.1 Azure Entra ID
Set up Azure Entra ID (formerly Azure Active Directory) to manage secure connections between the services.
Register an application in Azure Entra ID to allow permissions for your Airflow instance.



####
5.2 Personal Tokens and API Keys
Use Alpha Vantage API keys to securely pull stock data.
Store keys, tokens, and secrets securely in .env files or within Azure Key Vault to access them in your pipeline.
Create a .env file for storing secrets:

####
# .env file
ALPHA_VANTAGE_API_KEY="your_api_key"
POSTGRES_PASSWORD="your_postgres_password"
Make sure your code loads the .env file in Python using python-dotenv:


from dotenv import load_dotenv
load_dotenv()


####
***6. Develop the ETL Pipeline***

###
6.1 Data Extraction (Airflow + Python)
Use Airflow PythonOperator to automate the extraction of financial data from the Alpha Vantage API.
Incrementally extract the data by capturing the latest available data using timestamps.

####
6.2 Data Transformation (Azure Databricks + PySpark)
Set up Databricks jobs to transform and clean the raw stock data.
Perform operations like filtering, aggregation, and missing data handling using PySpark and pandas.

###
6.3 Data Loading (Azure Blob Storage + PostgreSQL)
Store the cleaned data in Azure Blob Storage for backup and persistence.
Finally, load the cleaned data into PostgreSQL for further analysis.
Implement checks for duplicate data before loading to ensure consistency.

####
***7. Logging and Error Tracking***
Incorporate logging in Airflow tasks to track errors and monitor successful runs:

import logging
logging.basicConfig(level=logging.INFO)
logging.info("Task has started")
logging.error("An error occurred")

###
***8. Running the Pipeline***
Start your Airflow DAG to execute the end-to-end process:


airflow dags trigger finexa_dag
This will:

Extract data from the Alpha Vantage API.
Transform it in Azure Databricks.
Load the final cleaned data into Azure Blob Storage and PostgreSQL.
Perform data validation and logging throughout the process.

###
***Conclusion***
This ETL pipeline project demonstrates how you can build a secure, scalable, and automated data pipeline using modern tools such as Apache Airflow, Azure Blob Storage, Databricks, and PostgreSQL. The focus on incremental data loading, duplicate removal, and data checks ensures that your data remains accurate and up to date. Logging and security measures were implemented to provide a production-ready solution.