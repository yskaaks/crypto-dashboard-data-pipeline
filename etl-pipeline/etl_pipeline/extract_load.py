import pandas as pd
import boto3
from prefect import task
from prefect.artifacts import create_table_artifact
from prefect_aws import AwsCredentials
import requests
import psycopg2
from dotenv import load_dotenv
import os
import logging
import json
from io import StringIO

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (pd.Timestamp, pd.Timedelta)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def dataframe_to_json_serializable(df):
    """Convert a DataFrame to a JSON-serializable format"""
    return json.loads(json.dumps(df.to_dict(orient="records"), default=json_serial))

@task(name="Extract Data from CoinGecko")
def extract_data_task():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": False
    }
    headers = {
        'accept': 'application/json',
        'x-cg-api-key': os.getenv('CG_API_KEY')
    }

    logger.info("Sending request to CoinGecko API")
    response = requests.get(url, headers=headers, params=params)
    logger.info(f"Received response with status code: {response.status_code}")

    if response.status_code != 200:
        logger.error(f"Failed to fetch data: {response.text}")
        return pd.DataFrame()  # Return an empty DataFrame on failure

    data = response.json()
    df = pd.DataFrame(data)
    logger.info(f"Extracted data: {df.head()}")

    # Load AWS credentials
    aws_credentials = AwsCredentials.load("my-aws-creds")
    logger.info(f"AWS Access Key ID: {aws_credentials.aws_access_key_id[:5]}...")
    logger.info(f"AWS Secret Access Key: {'*' * 5}...")  # Just log asterisks for the secret key

    s3 = boto3.client('s3', 
                      aws_access_key_id=aws_credentials.aws_access_key_id,
                      aws_secret_access_key=aws_credentials.aws_secret_access_key.get_secret_value(),
                      region_name='ap-southeast-2')  # Make sure this is your correct AWS region
    
    bucket_name = os.getenv('AWS_S3_BUCKET_RAW')
    file_name = f'raw_crypto_data_{pd.Timestamp.now().strftime("%Y%m%d%H%M%S")}.json'
    
    logger.info(f"Attempting to save data to S3 bucket: {bucket_name}")
    try:
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=json.dumps(data))
        logger.info(f"Raw data saved to S3: {bucket_name}/{file_name}")
    except Exception as e:
        logger.error(f"Failed to save data to S3: {str(e)}")

    create_table_artifact(
        key="extracted-data",
        table=dataframe_to_json_serializable(df),
        description="Extracted data from CoinGecko API"
    )

    return df

@task(name="Load Data into RDS")
def load_data_task(data: pd.DataFrame):
    if data.empty:
        logger.warning("No data to load into RDS")
        return

    aws_credentials = AwsCredentials.load("my-aws-creds")
    
    conn = psycopg2.connect(
        host=os.getenv('RDS_HOST'),
        database=os.getenv('RDS_DB_NAME'),
        user=os.getenv('RDS_USERNAME'),
        password=os.getenv('RDS_PASSWORD')
    )
    
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS crypto_data (
            id TEXT PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            current_price REAL,
            market_cap REAL,
            total_volume REAL,
            high_24h REAL,
            low_24h REAL,
            price_change_24h REAL,
            price_change_percentage_24h REAL,
            market_cap_change_24h REAL,
            market_cap_change_percentage_24h REAL,
            circulating_supply REAL,
            total_supply REAL,
            max_supply REAL,
            ath REAL,
            ath_change_percentage REAL,
            atl REAL,
            atl_change_percentage REAL,
            last_updated TIMESTAMP,
            ath_date TIMESTAMP,
            atl_date TIMESTAMP,
            volume_to_market_cap_ratio REAL,
            price_to_ath_ratio REAL,
            market_dominance REAL,
            has_max_supply BOOLEAN,
            circulating_supply_percentage REAL,
            days_since_ath INTEGER,
            market_cap_category TEXT,
            volatility REAL,
            significant_price_change BOOLEAN,
            year INTEGER,
            month INTEGER,
            roi_times REAL,
            roi_currency TEXT,
            roi_percentage REAL
        )
    ''')
    logger.info("Created or updated table crypto_data")
 # Get the current columns in the database table
    cur.execute("SELECT * FROM crypto_data LIMIT 0")
    db_columns = [desc[0] for desc in cur.description]

    # Filter the DataFrame to only include columns that exist in the database
    data_to_insert = data[[col for col in data.columns if col in db_columns]]

    # Use StringIO for efficient data insertion
    buffer = StringIO()
    data_to_insert.to_csv(buffer, index=False, header=False, na_rep='NULL')
    buffer.seek(0)

    try:
        cur.copy_from(buffer, 'crypto_data', sep=',', columns=data_to_insert.columns, null='NULL')
        conn.commit()
        logger.info(f"Data loaded into RDS successfully. Inserted {len(data_to_insert)} rows.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading data into RDS: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

    create_table_artifact(
        key="loaded-data",
        table=dataframe_to_json_serializable(data_to_insert.head(10)),
        description="Data loaded into RDS database (first 10 rows)"
    )