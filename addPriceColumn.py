import os
import pandas as pd
from datetime import datetime

def load_csv_files(historical_prices_file, rewards_file):
    """
    Loads the historical prices and rewards CSV files into DataFrames.
    Ensures proper handling of headers and date parsing.
    """
    # Load historical prices
    try:
        historical_prices = pd.read_csv(historical_prices_file)
        historical_prices.rename(columns=lambda x: x.strip().lower(), inplace=True)

        # Validate columns
        if "date" not in historical_prices.columns or "price" not in historical_prices.columns:
            raise KeyError(f"Columns 'date' and 'price' not found in {historical_prices_file}. Found: {historical_prices.columns}")

        # Parse dates
        historical_prices["date"] = pd.to_datetime(historical_prices["date"], format="%Y-%m-%d", errors="coerce")
        if historical_prices["date"].isna().any():
            raise ValueError(f"Invalid date format in {historical_prices_file}")
    except Exception as e:
        raise ValueError(f"Error loading historical prices file: {e}")
    
    # Load rewards
    try:
        rewards = pd.read_csv(rewards_file, header=None, names=["block", "date_time", "reward"])
        rewards["date_time"] = pd.to_datetime(rewards["date_time"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
        if rewards["date_time"].isna().any():
            raise ValueError(f"Invalid date_time format in {rewards_file}")
        
        rewards["reward"] = pd.to_numeric(rewards["reward"], errors="coerce")
        if rewards["reward"].isna().any():
            raise ValueError(f"Invalid reward format in {rewards_file}")
    except Exception as e:
        raise ValueError(f"Error loading rewards file: {e}")
    
    return historical_prices, rewards


def find_closest_price(date, historical_prices):
    """
    Finds the closest date in the historical prices DataFrame to the given date.
    """
    # Convert date to datetime to ensure compatibility
    date = pd.to_datetime(date)

    # Find the closest date and return its corresponding price
    closest_date = historical_prices.iloc[(historical_prices["date"] - date).abs().argsort()[:1]]
    return closest_date.iloc[0]["price"]

def calculate_cumulative_usd_rewards(historical_prices, rewards):
    """
    Calculates the USD rewards incrementally and ensures cumulative USD rewards never decrease.
    """
    cumulative_usd_rewards = 0  # Initialize cumulative USD rewards
    usd_rewards = []
    previous_reward = 0  # Keep track of the previous reward amount

    for _, row in rewards.iterrows():
        reward_date = row["date_time"].date()  # Get the date part only
        current_reward = row["reward"]
        
        # Find the closest price for the reward date
        price = find_closest_price(reward_date, historical_prices)
        
        # Calculate the incremental reward and its USD value
        incremental_reward = current_reward - previous_reward
        incremental_usd_value = (incremental_reward / 10**18) * price  # Adjust for decimals
        
        # Update cumulative USD rewards
        cumulative_usd_rewards += incremental_usd_value
        
        # Append the cumulative rewards to the list
        usd_rewards.append(cumulative_usd_rewards)
        
        # Update the previous reward for the next iteration
        previous_reward = current_reward
    
    rewards["usd_rewards"] = usd_rewards
    return rewards

def process_rewards_for_network(network_name):
    """
    Processes rewards for a specific network.
    """
    # Define file paths
    historical_prices_file = f"csv/historical_prices/{network_name}_historical_prices.csv"
    rewards_file = f"csv/lending_rewards/{network_name}_rewards.csv"
    
    # Check if files exist
    if not os.path.exists(historical_prices_file) or not os.path.exists(rewards_file):
        print(f"Missing files for network: {network_name}")
        return
    
    # Load data
    historical_prices, rewards = load_csv_files(historical_prices_file, rewards_file)
    
    # Calculate cumulative USD rewards
    updated_rewards = calculate_cumulative_usd_rewards(historical_prices, rewards)
    
    # Save the updated rewards file
    output_file = f"csv/lending_rewards/{network_name}_rewards_with_usd.csv"
    updated_rewards.to_csv(output_file, index=False)
    print(f"Processed rewards saved to {output_file}")

def main():
    # Process rewards for each network
    networks = ["fuse", "meter", "telos"]
    for network in networks:
        process_rewards_for_network(network)

if __name__ == "__main__":
    main()
