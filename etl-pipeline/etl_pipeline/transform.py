import pandas as pd
import numpy as np
from prefect import task
from prefect.artifacts import create_table_artifact
from prefect_aws import AwsCredentials
import boto3
import logging
from datetime import datetime
from typing import Dict, Any
import json
import os
from io import StringIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_convert(value: Any, convert_func: callable, default: Any = np.nan) -> Any:
    """Safely convert a value using the provided function, returning a default if it fails."""
    try:
        return convert_func(value)
    except (ValueError, TypeError):
        return default

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (pd.Timestamp, pd.Timedelta)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def dataframe_to_json_serializable(df):
    """Convert a DataFrame to a JSON-serializable format"""
    return json.loads(json.dumps(df.to_dict(orient="records"), default=json_serial))

@task(name="Transform Data", retries=3, retry_delay_seconds=30)
def transform_data_task(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        logger.warning("No data to transform")
        return data

    logger.info("Starting data transformation")
    logger.info(f"Initial data shape: {data.shape}")

    try:
        # Convert timestamps
        for col in ['last_updated', 'ath_date', 'atl_date']:
            if col in data.columns:
                data[col] = pd.to_datetime(data[col], utc=True)

        # Numeric columns to process
        numeric_columns = [
            'current_price', 'market_cap', 'fully_diluted_valuation', 'total_volume', 'high_24h', 'low_24h',
            'price_change_24h', 'price_change_percentage_24h', 'market_cap_change_24h',
            'market_cap_change_percentage_24h', 'circulating_supply', 'total_supply',
            'max_supply', 'ath', 'ath_change_percentage', 'atl', 'atl_change_percentage'
        ]

        for col in numeric_columns:
            if col in data.columns:
                data[col] = data[col].apply(lambda x: safe_convert(x, float))
                logger.info(f"Processed {col}: min={data[col].min()}, max={data[col].max()}, mean={data[col].mean()}")

        # Calculate additional metrics
        data['volume_to_market_cap_ratio'] = data['total_volume'] / data['market_cap']
        data['price_to_ath_ratio'] = data['current_price'] / data['ath']
        data['market_dominance'] = data['market_cap'] / data['market_cap'].sum()
        data['has_max_supply'] = data['max_supply'].notnull()
        data['circulating_supply_percentage'] = data['circulating_supply'] / data['total_supply'] * 100
        data['days_since_ath'] = (pd.Timestamp.now(tz='UTC') - data['ath_date']).dt.days
        
        # Categorize coins based on market cap
        data['market_cap_category'] = pd.cut(
            data['market_cap'],
            bins=[0, 1e9, 10e9, 100e9, np.inf],
            labels=['Small Cap', 'Mid Cap', 'Large Cap', 'Mega Cap']
        )

        # Volatility indicator (simplified)
        data['volatility'] = (data['high_24h'] - data['low_24h']) / data['low_24h']

        # Flag for significant price changes
        data['significant_price_change'] = np.abs(data['price_change_percentage_24h']) > 5

        # Extract year and month from last_updated for potential time-based analysis
        data['year'] = data['last_updated'].dt.year
        data['month'] = data['last_updated'].dt.month

        # Handle missing values
        data = data.fillna({
            'circulating_supply': 0,
            'total_supply': 0,
            'max_supply': 0
        })

        # Parse ROI column
        if 'roi' in data.columns:
            data['roi'] = data['roi'].apply(parse_roi)
            data['roi_times'] = data['roi'].apply(lambda x: x['times'])
            data['roi_currency'] = data['roi'].apply(lambda x: x['currency'])
            data['roi_percentage'] = data['roi'].apply(lambda x: x['percentage'])
            data = data.drop('roi', axis=1)

        # Ensure all coins have a rank (fill missing with max+1)
        data['market_cap_rank'] = data['market_cap_rank'].fillna(data['market_cap_rank'].max() + 1)

        # Log data quality metrics
        for col in data.columns:
            if data[col].dtype == 'object':
                # For object columns, just count non-null values
                non_null_count = data[col].count()
                logger.info(f"{col}: {len(data) - non_null_count} null values")
            else:
                null_count = data[col].isnull().sum()
                unique_count = data[col].nunique()
                logger.info(f"{col}: {null_count} null values, {unique_count} unique values")

        logger.info(f"Final data shape: {data.shape}")
        logger.info(f"Columns after transformation: {data.columns.tolist()}")

        # Save transformed data to S3
        aws_credentials = AwsCredentials.load("my-aws-creds")
        
        logger.info("AWS credentials loaded successfully")
        logger.info(f"AWS Access Key ID: {aws_credentials.aws_access_key_id[:5]}...")
        logger.info(f"AWS Secret Access Key length: {len(aws_credentials.aws_secret_access_key)}")
        
        s3 = boto3.client('s3', region_name='ap-southeast-2')
        
        bucket_name = os.getenv('AWS_S3_BUCKET_PROCESSED')
        file_name = f'transformed_crypto_data_{pd.Timestamp.now().strftime("%Y%m%d%H%M%S")}.csv'
        
        csv_buffer = StringIO()
        data.to_csv(csv_buffer)
        
        logger.info(f"Attempting to save transformed data to S3 bucket: {bucket_name}")
        try:
            s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
            logger.info(f"Transformed data saved to S3: {bucket_name}/{file_name}")
        except Exception as e:
            logger.error(f"Failed to save transformed data to S3: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            if hasattr(e, 'response'):
                logger.error(f"Error response: {e.response}")

        create_table_artifact(
            key="transformed-data",
            table=dataframe_to_json_serializable(data.head(10)),
            description="Transformed cryptocurrency market data (first 10 rows)"
        )

    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        raise

    return data

def parse_roi(roi: Dict[str, Any]) -> Dict[str, float]:
    """Parse ROI dictionary, handling potential missing or invalid values."""
    if not isinstance(roi, dict):
        return {'times': 0.0, 'currency': 'usd', 'percentage': 0.0}
    return {
        'times': safe_convert(roi.get('times', 0), float),
        'currency': roi.get('currency', 'usd'),
        'percentage': safe_convert(roi.get('percentage', 0), float)
    }