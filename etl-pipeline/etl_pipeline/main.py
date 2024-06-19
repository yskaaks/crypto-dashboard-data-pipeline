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
    logger.info("Extracted data step completed")

    transformed_data = transform_data_task(extracted_data)
    logger.info("Transformed data step completed")

    load_data_task(transformed_data)
    logger.info("Load data step completed")

if __name__ == "__main__":
    main()
