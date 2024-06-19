import pandas as pd
from prefect import task
from prefect.artifacts import create_table_artifact
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@task(name="Transform Data")
def transform_data_task(data: pd.DataFrame):
    if data.empty:
        logger.warning("No data to transform")
        return data

    logger.info("Starting data transformation")
    data.dropna(inplace=True)
    data['current_price'] = pd.to_numeric(data['current_price'], errors='coerce')
    data['market_cap'] = pd.to_numeric(data['market_cap'], errors='coerce')
    data['total_volume'] = pd.to_numeric(data['total_volume'], errors='coerce')
    data['high_24h'] = pd.to_numeric(data['high_24h'], errors='coerce')
    data['low_24h'] = pd.to_numeric(data['low_24h'], errors='coerce')
    data['price_change_24h'] = pd.to_numeric(data['price_change_24h'], errors='coerce')
    data['price_change_percentage_24h'] = pd.to_numeric(data['price_change_percentage_24h'], errors='coerce')
    data['market_cap_change_24h'] = pd.to_numeric(data['market_cap_change_24h'], errors='coerce')
    data['market_cap_change_percentage_24h'] = pd.to_numeric(data['market_cap_change_percentage_24h'], errors='coerce')

    logger.info(f"Transformed data: {data.head()}")

    create_table_artifact(
        key="transformed-data",
        table=data.to_dict(orient="records"),
        description="Transformed data"
    )

    return data
