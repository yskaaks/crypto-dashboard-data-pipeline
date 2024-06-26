from prefect import flow
from extract_load import extract_data_task, load_data_task
from transform import transform_data_task
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@flow(name="Crypto ETL Pipeline", log_prints=True)
def main():
    logger.info("Starting ETL process")

    extracted_data = extract_data_task()
    logger.info(f"Extracted data shape: {extracted_data.shape}")

    transformed_data = transform_data_task(extracted_data)
    logger.info(f"Transformed data shape: {transformed_data.shape}")

    load_data_task(transformed_data)
    logger.info("ETL process completed")

if __name__ == "__main__":
    main()