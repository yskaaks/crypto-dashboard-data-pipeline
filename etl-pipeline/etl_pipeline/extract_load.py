import pandas as pd
from prefect import task
import requests
import sqlite3
from prefect.artifacts import create_table_artifact
from dotenv import load_dotenv
import os
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

    create_table_artifact(
        key="extracted-data",
        table=df.to_dict(orient="records"),
        description="Extracted data from CoinGecko API"
    )

    return df

@task(name="Load Data into SQLite")
def load_data_task(data: pd.DataFrame):
    if data.empty:
        logger.warning("No data to load into SQLite")
        return

    conn = sqlite3.connect(os.getenv('DB_PATH'))
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
            market_cap_change_percentage_24h REAL
        )
    ''')
    logger.info("Created table crypto_data if not exists")

    for index, row in data.iterrows():
        cur.execute('''
            INSERT OR REPLACE INTO crypto_data (
                id, symbol, name, current_price, market_cap, total_volume,
                high_24h, low_24h, price_change_24h, price_change_percentage_24h,
                market_cap_change_24h, market_cap_change_percentage_24h
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['id'], row['symbol'], row['name'], row['current_price'], row['market_cap'],
            row['total_volume'], row['high_24h'], row['low_24h'], row['price_change_24h'],
            row['price_change_percentage_24h'], row['market_cap_change_24h'],
            row['market_cap_change_percentage_24h']
        ))
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Data loaded into SQLite successfully")

    create_table_artifact(
        key="loaded-data",
        table=data.to_dict(orient="records"),
        description="Data loaded into SQLite database"
    )
