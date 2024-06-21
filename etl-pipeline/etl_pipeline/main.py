from datetime import timedelta, date, datetime, timezone
from prefect import flow
from extract_load import extract_data_task, load_data_task
from transform import transform_data_task
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@flow(name="Main Flow", log_prints=True)
def main():
    logger.info("Starting ETL process")
    start_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    end_date = datetime.now(timezone.utc).date()

    extracted_data = extract_data_task()
    logger.info(f"Extracted data shape: {extracted_data.shape}")
    logger.info(f"Extracted data columns: {extracted_data.columns}")
    logger.info(f"Extracted data types:\n{extracted_data.dtypes}")
    logger.info(f"Extracted data sample:\n{extracted_data.head()}")

    transformed_data = transform_data_task(extracted_data)
    logger.info(f"Transformed data shape: {transformed_data.shape}")
    logger.info(f"Transformed data columns: {transformed_data.columns}")
    logger.info(f"Transformed data types:\n{transformed_data.dtypes}")
    logger.info(f"Transformed data sample:\n{transformed_data.head()}")
    
    # Add detailed info about numeric columns
    numeric_columns = [
        'current_price', 'market_cap', 'total_volume', 'high_24h', 'low_24h',
        'price_change_24h', 'price_change_percentage_24h',
        'market_cap_change_24h', 'market_cap_change_percentage_24h'
    ]
    for col in numeric_columns:
        if col in transformed_data.columns:
            logger.info(f"{col}: min={transformed_data[col].min()}, max={transformed_data[col].max()}, mean={transformed_data[col].mean()}")

    load_data_task(transformed_data)
    logger.info("Load data step completed")

if __name__ == "__main__":
    main()