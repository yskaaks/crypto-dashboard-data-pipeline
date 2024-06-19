import pandas as pd
from etl_pipeline.transform import transform_data_task

def test_transform_data_task():
    data = pd.DataFrame({
        'id': ['bitcoin', 'ethereum'],
        'symbol': ['btc', 'eth'],
        'name': ['Bitcoin', 'Ethereum'],
        'current_price': [40000, None],
        'market_cap': [700000000000, None],
        'total_volume': [35000000000, None],
        'high_24h': [41000, None],
        'low_24h': [39000, None],
        'price_change_24h': [1000, None],
        'price_change_percentage_24h': [2.5, None],
        'market_cap_change_24h': [15000000000, None],
        'market_cap_change_percentage_24h': [2.2, None]
    })
    transformed_data = transform_data_task.run(data)
    assert transformed_data['current_price'].dtype == 'float64'
    assert transformed_data['market_cap'].dtype == 'float64'
    assert transformed_data['total_volume'].dtype == 'float64'
    assert transformed_data.isnull().sum().sum() == 0
