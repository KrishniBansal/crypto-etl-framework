import requests
import pandas as pd
from datetime import datetime

def fetch_live_bitcoin_data():
    try:
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_vol=true&include_24hr_high_low=true"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()['bitcoin']
        
        live_df = pd.DataFrame([{
            'Timestamp': datetime.now(),
            'Open': data['usd'],
            'High': data['usd_24h_high'],
            'Low': data['usd_24h_low'],
            'Close': data['usd'],
            'Volume': data['usd_24h_vol'], # Matching your Silver Layer column name
            'Weighted_Price': data['usd']
        }])
        return live_df
    except Exception as e:
        print(f"API Fetch Failed: {e}")
        return pd.DataFrame() # Return empty if API fails