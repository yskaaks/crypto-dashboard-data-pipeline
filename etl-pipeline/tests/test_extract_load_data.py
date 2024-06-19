import pandas as pd
from etl_pipeline.extract_load import extract_data_task, load_data_task
from unittest import mock
import sqlite3
import pytest

@pytest.fixture
def crypto_data():
    return pd.DataFrame({
        'id': ['bitcoin', 'ethereum'],
        'symbol': ['btc', 'eth'],
        'name': ['Bitcoin', 'Ethereum'],
        'current_price': [40000, 3000],
        'market_cap': [700000000000, 300000000000],
        'total_volume': [35000000000, 20000000000],
        'high_24h': [41000, 3100],
        'low_24h': [39000, 2900],
        'price_change_24h': [1000, 100],
        'price_change_percentage_24h': [2.5, 3.3],
        'market_cap_change_24h': [15000000000, 10000000000],
        'market_cap_change_percentage_24h': [2.2, 3.4]
    })

def test_extract_data_task(crypto_data):
    with mock.patch('requests.get') as mock_get:
        mock_response = mock.Mock()
        mock_response.json.return_value = crypto_data.to_dict(orient='records')
        mock_get.return_value = mock_response
        data = extract_data_task.run()
        assert not data.empty

def test_load_data_task(crypto_data):
    with mock.patch('sqlite3.connect') as mock_connect:
        mock_conn = mock_connect.return_value
        mock_cur = mock_conn.cursor.return_value
        load_data_task.run(crypto_data)
        assert mock_cur.execute.called
        assert mock_conn.commit.called
