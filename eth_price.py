import yfinance as yf
import pandas as pd

def fetch_eth_prices():
    """
    Fetch historical open prices for ETH from Yahoo Finance and save to a clean CSV file.
    """
    # Fetch historical data for ETH/USD
    eth_data = yf.download("ETH-USD", start="2021-01-01", end="2023-01-01", progress=False)
    
    # Extract relevant columns: Date and Open price
    eth_prices = eth_data[["Open"]].reset_index()
    eth_prices.rename(columns={"Open": "price", "Date": "date"}, inplace=True)
    
    # Ensure only "date" and "price" columns are included
    eth_prices = eth_prices[["date", "price"]]
    
    # Save to CSV
    filename = "eth_prices_clean.csv"
    eth_prices.to_csv(filename, index=False)
    print(f"Data saved to {filename}")
    
    # Print first few rows
    print(eth_prices.head())

if __name__ == "__main__":
    fetch_eth_prices()
