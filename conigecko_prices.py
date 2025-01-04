import requests
import csv
from datetime import datetime, timedelta

# Function to fetch historical data from CoinGecko API
def fetch_historical_prices(coin_id, currency, from_date, to_date):
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {
        'vs_currency': currency,
        'from': from_date.timestamp(),
        'to': to_date.timestamp()
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()['prices']
    else:
        raise Exception(f"Failed to fetch data: {response.status_code} {response.text}")

# Function to save data to a CSV file
def save_to_csv(filename, data):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Date', 'Price'])  # Header row
        writer.writerows(data)

# Main script
if __name__ == "__main__":
    # Define parameters for each network
    networks = [
        {"coin_id": "telos", "filename": "telos_historical_prices.csv"},
        {"coin_id": "fuse-network-token", "filename": "fuse_historical_prices.csv"},
        {"coin_id": "meter", "filename": "meter_historical_prices.csv"}
    ]
    currency = "usd"  # Currency for prices
    to_date = datetime.now()
    from_date = to_date - timedelta(days=365)  # 2 years ago

    for network in networks:
        try:
            print(f"Fetching data for {network['coin_id']} from CoinGecko...")
            prices = fetch_historical_prices(network['coin_id'], currency, from_date, to_date)

            # Process the data into a CSV-friendly format
            formatted_data = [
                [datetime.utcfromtimestamp(entry[0] / 1000).strftime('%Y-%m-%d'), entry[1]]
                for entry in prices
            ]

            # Save to CSV
            save_to_csv(network['filename'], formatted_data)
            print(f"Data saved to {network['filename']}")

        except Exception as e:
            print(f"An error occurred for {network['coin_id']}: {e}")