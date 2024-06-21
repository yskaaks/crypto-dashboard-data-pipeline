import pandas as pd
import numpy as np
from prefect import task
from prefect.artifacts import create_table_artifact
import logging
from datetime import datetime
from typing import Dict, Any
import json

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
        # ... (rest of the transformation code remains the same)

        logger.info(f"Final data shape: {data.shape}")
        logger.info(f"Columns after transformation: {data.columns.tolist()}")

        create_table_artifact(
            key="transformed-data",
            table=dataframe_to_json_serializable(data.head(10)),
            description="Transformed cryptocurrency market data (first 10 rows)"
        )

    except Exception as e:
        logger.error(f"Error during transformation: {e}")
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