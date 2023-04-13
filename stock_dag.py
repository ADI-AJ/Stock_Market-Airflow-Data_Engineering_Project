# Importing necessary packages for the ETL operations
#import yfinance as yf
import pandas as pd
from airflow.operators.bash import BashOperator
import boto3
# Boto3 is the name of the Python SDK for AWS. It allows you to directly create, update, and delete AWS resources from your Python scripts

# Importing necessary packages for airflow operations
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime,date

f_name='stock_data-'+ str(date.today()) + '.csv' # Name for the csv file to be stored

# Function for extracting data of US top tech stocks using yfinance API
def run_stock_etl():
    import yfinance as yf
    # Define a list of tickers
    tickers = ["AAPL", "GOOG", "MSFT", "AMZN", "FB"]

    # Create an empty DataFrame to store the data
    data = pd.DataFrame()

    # Download data for each ticker and append it to the DataFrame
    for ticker in tickers:
        stock = yf.Ticker(ticker)
        stock_data = stock.history(period="1d")
        stock_data["Ticker"] = ticker
        data = data.append(stock_data)

    # Saving dataframe as a csv file
    data.to_csv(f"C:\\Users\\miles.MILE-BL-4378-LA.001\\Documents\\materials\\source\\{f_name}",index=False)

# Function to load csv from local system to AWS s3
def to_s3():
    s3 = boto3.client('s3', aws_access_key_id='AKIAXMY62HNE7DLV2XRV', aws_secret_access_key='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')

    # Upload a file to S3
    bucket_name = 'airflow-stock-bucket'
    file_name = f"C:\\Users\\miles.MILE-BL-4378-LA.001\\Documents\\materials\\source\\{f_name}"
    object_name = f_name
    s3.upload_file(file_name, bucket_name, object_name)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':datetime(2023,4,12,17),
    'email':['adithya.aj6@gmail.com'],
    'email_on_failure':True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
    }

dag = DAG(
    'stock_dag',
    default_args = default_args,
    description='Top tech stocks from yfinance to S3',
    schedule_interval='@daily'
)

task_1 = BashOperator(
    task_id = 'installing_yfinance',
    bash_command='pip install yfinance',
    dag=dag
)

task_2 = PythonOperator(
    task_id = 'get_data_as_DF',
    python_callable=run_stock_etl,
    dag=dag
)

task_3 = PythonOperator(
    task_id = 'push_csv_to_s3',
    python_callable=to_s3,
    dag=dag
)

# Orchestrating data pipeline
task_1 >> task_2 >> task_3
