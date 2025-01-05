import requests
import os
import pandas as pd

def fetch_historical_prices(coin_id, currency, from_date, to_date):
    """
    Fetches historical price data from the CoinGecko API for a given coin_id.
    Returns a list of [timestamp_millis, price].
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {
        'vs_currency': currency,
        'from': from_date.timestamp(),
        'to': to_date.timestamp()
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if 'prices' in data:
            return data['prices']  # list of [timestamp_millis, price]
        else:
            raise Exception("No 'prices' key found in CoinGecko response.")
    else:
        raise Exception(f"Failed to fetch data: {response.status_code} {response.text}")
 
 
def load_historical_prices(historical_prices_file):
    """
    Loads the historical prices into a DataFrame with columns: date, price.
    """
    if not os.path.exists(historical_prices_file):
        raise FileNotFoundError(f"Missing historical prices file: {historical_prices_file}")

    df = pd.read_csv(historical_prices_file)
    df.columns = [c.strip().lower() for c in df.columns]
    if "date" not in df.columns or "price" not in df.columns:
        raise KeyError(f"Columns 'date' and 'price' not found in {historical_prices_file}.")

    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    if df["date"].isna().any():
        raise ValueError(f"Invalid date format in {historical_prices_file}")

    return df   
    
def find_closest_price(date, historical_prices_df):
    """
    Finds the closest date in historical_prices_df and returns the 'price'.
    """
    date = pd.to_datetime(date)
    diffs = (historical_prices_df["date"] - date).abs()
    idx = diffs.idxmin()
    return historical_prices_df.loc[idx, "price"]