import os
import csv
import time
import json
import requests
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from web3 import Web3
from helper import *

###############################################################################
# CONFIGURATION
###############################################################################

def load_config(config_file):
    with open(config_file, "r") as f:
        return json.load(f)

CONFIG = load_config("config.json")

# CoinGecko coin IDs for each network (update if needed)
COIN_IDS = {
    "meter": "meter",
    "telos": "telos",
    "fuse": "fuse-network-token"
}

# Minimal ABI that includes the RewardsAccrued event signature
contract_abi = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "user", "type": "address"},
            {"indexed": False, "name": "amount", "type": "uint256"}
        ],
        "name": "RewardsAccrued",
        "type": "event"
    }
]
###############################################################################
# 1) FETCH OR GENERATE HISTORICAL PRICES INCREMENTALLY
###############################################################################

def generate_fixed_telos_prices(from_date, to_date):
    """
    Creates a list of [date, 1.0] from from_date to to_date (daily).
    This is used for Telos where price is pinned to $1.
    """
    # Ensure both from_date and to_date are datetime.date objects
    if isinstance(from_date, datetime):
        from_date = from_date.date()
    if isinstance(to_date, datetime):
        to_date = to_date.date()

    day_count = (to_date - from_date).days + 1
    data = []
    current_day = from_date
    for _ in range(day_count):
        date_str = current_day.strftime('%Y-%m-%d')
        data.append([date_str, 1.0])  # Price always 1
        current_day += timedelta(days=1)
    return data

def save_to_csv(filename, data):
    """
    Saves a list of [date, price] rows to a CSV file, overwriting existing content.
    Headers are written in lowercase: 'date', 'price'.
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['date', 'price'])  # Header row in lowercase
        writer.writerows(data)

def update_historical_csv(network_name, coin_id, from_date, to_date, csv_path):
    """
    Incrementally updates the historical prices CSV. Steps:
      1) If CSV doesn't exist, create it from scratch (entire range).
      2) If CSV does exist, find the latest date in it and fetch only NEW data.
      3) Append, deduplicate, and save.
    """
    # Helper function to fetch from CoinGecko (like in helper.py)
    def fetch_coingecko_data(coin_id, currency, start_dt, end_dt):
        print(coin_id, currency, start_dt, end_dt, "inputs")
        try:
            prices = fetch_historical_prices(coin_id, currency, start_dt, end_dt)  # from helper.py
        except Exception as e:
            raise Exception(f"CoinGecko fetch failed: {e}")

        # Convert to [date, price] in daily increments
        formatted = []
        for p in prices:
            # p is [timestamp_millis, price]
            date_str = datetime.utcfromtimestamp(p[0] / 1000).strftime('%Y-%m-%d')
            formatted.append([date_str, p[1]])
        return formatted

    # 1) If CSV does not exist, create from scratch (full from_date->to_date)
    if not os.path.exists(csv_path):
        print(f"[INFO] No existing price CSV for {network_name}, creating from scratch...")
        if network_name.lower() == "telos":
            new_data = generate_fixed_telos_prices(from_date, to_date)
        else:
            new_data = fetch_coingecko_data(coin_id, "usd", from_date, to_date)
        save_to_csv(csv_path, new_data)
        return

    # 2) CSV exists -> load it
    existing_df = pd.read_csv(csv_path)
    existing_df.columns = [c.strip().lower() for c in existing_df.columns]  # Convert headers to lowercase

    # Debugging: Print the columns after processing
    print(f"Columns after processing in {csv_path}: {existing_df.columns.tolist()}")

    if "date" not in existing_df.columns or "price" not in existing_df.columns:
        raise ValueError(f"{csv_path} does not have the required 'date'/'price' columns.")

    existing_df['date'] = pd.to_datetime(existing_df['date'], format='%Y-%m-%d', errors='coerce')
    existing_df.sort_values('date', inplace=True)
    existing_latest_date = existing_df['date'].max().date()

    # 3) If existing data is already up to or beyond 'to_date', nothing to fetch
    if existing_latest_date >= to_date:
        print(f"[INFO] {network_name} price CSV is already up to date. Latest date: {existing_latest_date}")
        return

    # 4) Otherwise, fetch only from (existing_latest_date + 1 day) to to_date
    new_start_date = existing_latest_date + timedelta(days=1)
    if network_name.lower() == "telos":
        new_data = generate_fixed_telos_prices(new_start_date, to_date)
    else:
        new_data = fetch_coingecko_data(coin_id, "usd", new_start_date, to_date)

    # 5) Convert new_data -> DataFrame, merge with existing
    df_new = pd.DataFrame(new_data, columns=["date", "price"])
    df_new["date"] = pd.to_datetime(df_new["date"], format='%Y-%m-%d', errors='coerce')

    merged = pd.concat([existing_df, df_new], ignore_index=True)
    merged.drop_duplicates(subset=["date"], keep='last', inplace=True)
    merged.sort_values('date', inplace=True)

    # 6) Save final
    merged_rows = merged[["date", "price"]].values.tolist()
    out_rows = []
    for row in merged_rows:
        dt_str = row[0].strftime('%Y-%m-%d')
        price = row[1]
        out_rows.append([dt_str, price])

    save_to_csv(csv_path, out_rows)
    print(f"[INFO] Updated {csv_path} from {new_start_date} to {to_date}.")

###############################################################################
# 2) FETCH ON-CHAIN REWARDS (Using Web3, chunked approach)
###############################################################################

def setup_web3(rpc_urls):
    """
    Sets up a Web3 instance using the first working RPC URL from the list.
    """
    for rpc in rpc_urls:
        web3 = Web3(Web3.HTTPProvider(rpc))
        if web3.isConnected():
            return web3
    raise ConnectionError(f"Could not connect to any RPC URL: {rpc_urls}")

def load_existing_data(csv_file):
    """
    Reads existing CSV data for (block, date_time, reward) and returns them
    as three lists. If the file doesn't exist or is empty, returns empty lists.
    """
    if not os.path.isfile(csv_file):
        return [], [], []

    blocks_list = []
    dates_list = []
    rewards_list = []

    with open(csv_file, "r", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # Skip header if exists
        for row in reader:
            # Expect row to be: [block, date_time, reward]
            if len(row) < 3:
                continue
            try:
                block = int(row[0])
                date = row[1]
                reward = float(row[2])  # Changed to float for USD rewards
            except ValueError:
                continue  # Skip rows with invalid data
            blocks_list.append(block)
            dates_list.append(date)
            rewards_list.append(reward)

    return blocks_list, dates_list, rewards_list

def append_data_to_csv(csv_file, block_num, date_time, cum_rewards):
    """
    Appends a single row [block, date_time, reward] to the CSV.
    """
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)
    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([block_num, date_time, cum_rewards])

def fetch_block_date(web3, block_num):
    """
    Fetches the timestamp for a given block and converts it to a human-readable date.
    """
    block = web3.eth.getBlock(block_num)
    timestamp = block.timestamp
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

def fetch_rewards_in_range(web3, contract, event_signature, from_block, to_block):
    """
    Fetches RewardsAccrued events between from_block and to_block.
    Returns total rewards found in that block range.
    """
    total_rewards = 0

    logs = web3.eth.get_logs({
        "fromBlock": from_block,
        "toBlock": to_block,
        "address": contract.address,
        "topics": [event_signature]
    })

    event = contract.events.RewardsAccrued()

    for entry in logs:
        decoded = event.processLog(entry)
        amount = decoded["args"]["amount"]
        total_rewards += amount

    return total_rewards

def process_controller(network_name, version_label, controller_config, rpc_urls):
    """
    Processes a single incentives controller (whether it's named v1, v2, or single).
    Generates/updates a CSV with columns [block, date_time, reward].
    """
    if version_label:
        print(f"\nProcessing {network_name.upper()} - {version_label.upper()}...")
        csv_file = f"csv/lending_rewards/{network_name}_{version_label}_rewards.csv"
    else:
        print(f"\nProcessing {network_name.upper()}...")
        csv_file = f"csv/lending_rewards/{network_name}_rewards.csv"

    contract_address = controller_config["address"]
    default_start_block = controller_config["default_start_block"]
    chunk_size = controller_config.get("chunk_size", 100_000)

    # Setup Web3
    web3 = setup_web3(rpc_urls)
    contract = web3.eth.contract(address=contract_address, abi=contract_abi)
    event_signature = web3.keccak(text="RewardsAccrued(address,uint256)").hex()

    # Load existing data (if any)
    blocks_list, dates_list, rewards_list = load_existing_data(csv_file)

    # Determine the starting point
    if len(blocks_list) == 0:
        current_block = default_start_block
        cumulative_rewards = 0.0
    else:
        current_block = blocks_list[-1] + 1
        cumulative_rewards = rewards_list[-1]

    latest_block = web3.eth.blockNumber
    if current_block > latest_block:
        print(f"No new blocks to process for {network_name.upper()} ({version_label}).")
        return

    print(f"Fetching data from block {current_block} to {latest_block}...")

    while current_block <= latest_block:
        end_block = min(current_block + chunk_size - 1, latest_block)
        print(f"  Processing blocks [{current_block} -> {end_block}]...")

        # Fetch rewards in this block range
        chunk_rewards = fetch_rewards_in_range(web3, contract, event_signature, current_block, end_block)

        # Update cumulative
        cumulative_rewards += chunk_rewards / 10**18  # Assuming rewards have 18 decimals

        # Fetch block date (we'll label it by the end_block's timestamp)
        date_time_str = fetch_block_date(web3, end_block)

        # Append to CSV
        append_data_to_csv(csv_file, end_block, date_time_str, cumulative_rewards)

        current_block = end_block + 1
        time.sleep(0.2)  # optional sleep to avoid spamming the RPC

    print(f"Finished processing {network_name.upper()} ({version_label}).")

def process_network(network_name, network_config):
    """
    Processes all incentives controllers for a given network,
    supporting both single or multiple controllers (v1, v2, etc.).
    """
    controllers = network_config["incentives_controller"]
    rpc_urls = network_config["rpcs"]

    # If there's a top-level "address" key, it's a single controller.
    if "address" in controllers and "default_start_block" in controllers:
        process_controller(network_name, None, controllers, rpc_urls)
    else:
        # Otherwise, multiple versions exist, e.g. "v1", "v2".
        for version, controller_config in controllers.items():
            process_controller(network_name, version, controller_config, rpc_urls)

###############################################################################
# 3) PARTIAL POST-PROCESSING FOR USD (Avoid Overwriting Old Data)
###############################################################################

def load_rewards_file(rewards_file):
    """
    Loads the raw rewards CSV file (block, date_time, reward).
    """
    if not os.path.exists(rewards_file):
        raise FileNotFoundError(f"Missing rewards file: {rewards_file}")

    df = pd.read_csv(rewards_file, header=None, names=["block", "date_time", "reward"])
    df["date_time"] = pd.to_datetime(df["date_time"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    if df["date_time"].isna().any():
        raise ValueError(f"Invalid date_time format in {rewards_file}")

    df["reward"] = pd.to_numeric(df["reward"], errors="coerce")
    if df["reward"].isna().any():
        raise ValueError(f"Invalid reward format in {rewards_file}")

    return df

def load_existing_usd_file(usd_file):
    """
    Loads an existing USD file if present, which has columns:
    block, date_time, reward, usd_rewards
    Returns a DataFrame or None if file doesn't exist.
    """
    if not os.path.exists(usd_file):
        return None

    df = pd.read_csv(usd_file)
    # We expect columns: block, date_time, reward, usd_rewards
    if "block" not in df.columns or "usd_rewards" not in df.columns:
        raise ValueError(f"{usd_file} does not contain expected columns (block, usd_rewards).")
    return df

def load_historical_prices(csv_path):
    """
    Loads a CSV with columns: date, price.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing historical prices file: {csv_path}")

    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]  # Convert headers to lowercase
    if "date" not in df.columns or "price" not in df.columns:
        raise KeyError(f"Missing 'date' or 'price' columns in {csv_path}")

    df["date"] = pd.to_datetime(df["date"], format='%Y-%m-%d', errors="coerce")
    if df["date"].isna().any():
        raise ValueError(f"Invalid date format in {csv_path}")
    return df

def find_closest_price(block_date, historical_prices_df):
    """
    Finds the closest date in `historical_prices_df` to `block_date` and
    returns the corresponding price (float).
    """
    # Ensure block_date is a datetime.date object
    if isinstance(block_date, datetime):
        block_date = block_date.date()

    # Convert to Timestamp for comparison
    block_date_only = pd.Timestamp(block_date)

    # Calculate absolute difference and find the closest date
    diffs = (historical_prices_df["date"] - block_date_only).abs()
    idx = diffs.idxmin()
    return float(historical_prices_df.loc[idx, "price"])

def calculate_new_usd_rows(new_rows_df, historical_prices_df, starting_token_reward, starting_cumulative_usd):
    """
    Given the new rows (with block, date_time, reward), calculates only
    the new incremental USD values. Returns a DataFrame with a 'usd_rewards' column.
    """
    cumulative_usd = starting_cumulative_usd
    previous_reward = starting_token_reward
    usd_list = []

    for _, row in new_rows_df.iterrows():
        # Convert date_time to just date
        reward_date = row["date_time"].date()
        current_reward = row["reward"]

        # Find price
        price = find_closest_price(row["date_time"], historical_prices_df)

        # Incremental difference from the previous row
        incremental_reward = current_reward - previous_reward
        if incremental_reward < 0:
            incremental_reward = 0  # Prevent negative increments

        # If token has 18 decimals, adjust
        incremental_usd_value = (incremental_reward / 10**18) * price

        cumulative_usd += incremental_usd_value
        usd_list.append(cumulative_usd)

        previous_reward = current_reward

    new_rows_df["usd_rewards"] = usd_list
    return new_rows_df

def process_rewards_for_network(network_name):
    """
    Loads the raw rewards CSV and the historical prices CSV.
    If a USD file exists, only appends new data (does not recalc old data).
    """
    hist_file = f"csv/historical_prices/{network_name}_historical_prices.csv"
    rewards_file = f"csv/lending_rewards/{network_name}_rewards.csv"
    usd_file = f"csv/lending_rewards/{network_name}_rewards_with_usd.csv"

    # 1) Load the needed files
    try:
        historical_prices_df = load_historical_prices(hist_file)
        rewards_df = load_rewards_file(rewards_file)
    except (FileNotFoundError, ValueError, KeyError) as e:
        print(f"Skipping USD calculation for {network_name}: {e}")
        return

    existing_usd_df = load_existing_usd_file(usd_file)

    if existing_usd_df is None:
        # No existing USD file -> do the full calc
        print(f"No existing USD data for {network_name}, calculating from scratch...")

        new_data_df = calculate_new_usd_rows(
            new_rows_df=rewards_df,
            historical_prices_df=historical_prices_df,
            starting_token_reward=0,
            starting_cumulative_usd=0.0
        )
        # Save it
        new_data_df.to_csv(usd_file, index=False)
        print(f"Processed rewards saved to {usd_file}")
    else:
        # We have existing data with 'usd_rewards' up to some block
        last_block = existing_usd_df["block"].max()
        print(f"Found existing USD data for {network_name} up to block {last_block}.")

        # Filter "rewards_df" to only the new blocks
        new_rows_df = rewards_df[rewards_df["block"] > last_block].copy()
        if new_rows_df.empty:
            print(f"No new rows to process for {network_name}.")
            return

        # We'll pick up from the last row in existing_usd_df
        last_row = existing_usd_df[existing_usd_df["block"] == last_block].iloc[-1]
        starting_token_reward = last_row["reward"]
        starting_cumulative_usd = last_row["usd_rewards"]

        # Calculate only for new rows
        new_data_df = calculate_new_usd_rows(
            new_rows_df=new_rows_df,
            historical_prices_df=historical_prices_df,
            starting_token_reward=starting_token_reward,
            starting_cumulative_usd=starting_cumulative_usd
        )

        # Combine the old data with the newly computed rows
        combined = pd.concat([existing_usd_df, new_data_df], ignore_index=True)
        # Sort by block (just in case)
        combined.sort_values(by="block", inplace=True)
        combined.to_csv(usd_file, index=False)
        print(f"Appended new rows to existing USD file: {usd_file}")

###############################################################################
# 4) GENERATE COMBINED LENDING INCENTIVES
###############################################################################

def generate_combined_lending_incentives(network_names, combined_csv_path):
    """
    Aggregates USD rewards from multiple networks into a combined CSV.
    The combined CSV will have columns: date_time, combined_usd_rewards.
    """
    print("\n=== GENERATING CUMULATIVE LENDING INCENTIVES ===")

    # Initialize an empty DataFrame for aggregation
    combined_df = pd.DataFrame()

    # Iterate over each network and merge data
    for network in network_names:
        usd_file = f"csv/lending_rewards/{network}_rewards_with_usd.csv"
        try:
            df = pd.read_csv(usd_file, parse_dates=["date_time"])
            if "usd_rewards" not in df.columns:
                print(f"Warning: 'usd_rewards' column missing in {usd_file}. Skipping.")
                continue
            # Select relevant columns
            df = df[["date_time", "usd_rewards"]].copy()
            df.rename(columns={"usd_rewards": f"{network}_usd_rewards"}, inplace=True)
            # Set date_time as index for merging
            df.set_index("date_time", inplace=True)
            if combined_df.empty:
                combined_df = df
            else:
                combined_df = combined_df.join(df, how="outer")
        except FileNotFoundError:
            print(f"Warning: USD rewards file for {network} not found at {usd_file}. Skipping.")
        except Exception as e:
            print(f"Error processing {usd_file}: {e}")

    if combined_df.empty:
        print("No data available to generate cumulative lending incentives.")
        return

    # Replace NaN with 0 for aggregation
    combined_df.fillna(0, inplace=True)

    # Calculate combined rewards by summing across networks
    combined_df["combined_usd_rewards"] = combined_df.sum(axis=1)

    # Reset index to have date_time as a column
    combined_df.reset_index(inplace=True)

    # Sort by date_time
    combined_df.sort_values(by="date_time", inplace=True)

    # Load existing combined CSV if it exists to append only new data
    if os.path.exists(combined_csv_path):
        existing_combined_df = pd.read_csv(combined_csv_path, parse_dates=["date_time"])
        # Find the latest date in existing_combined_df
        existing_latest_date = existing_combined_df["date_time"].max()
        # Filter combined_df for dates after existing_latest_date
        new_data_df = combined_df[combined_df["date_time"] > existing_latest_date].copy()
        if new_data_df.empty:
            print("Combined lending incentives CSV is already up to date.")
            return
        # Append new data
        updated_combined_df = pd.concat([existing_combined_df, new_data_df], ignore_index=True)
    else:
        # No existing combined CSV, use all data
        updated_combined_df = combined_df.copy()

    # Save the combined CSV
    os.makedirs(os.path.dirname(combined_csv_path), exist_ok=True)
    updated_combined_df.to_csv(combined_csv_path, index=False)
    print(f"Cumulative lending incentives saved to {combined_csv_path}")

###############################################################################
# MAIN
###############################################################################

def main():
    """
    Orchestrates the entire flow:
    1) Fetch on-chain rewards for each network (and store in CSV).
    2) Incrementally fetch/generate historical prices for each network:
       - If network is 'telos', we skip CoinGecko and use $1 price daily.
       - Otherwise, fetch from CoinGecko.
       - We only fetch new data beyond what's already in the CSV.
    3) Post-process to produce/update a final CSV with cumulative USD rewards
       only for newly added blocks.
    4) Generate a cumulative lending incentives CSV aggregating all networks.
    """

    # Create directories if they don't exist
    os.makedirs("csv/historical_prices", exist_ok=True)
    os.makedirs("csv/lending_rewards", exist_ok=True)

    # 1) Fetch on-chain rewards
    print("=== FETCHING ON-CHAIN REWARDS ===")
    for network_name, network_config in CONFIG.items():
        process_network(network_name, network_config)

    # 2) Historical prices: either fetch from CoinGecko or generate fixed $1 (Telos),
    #    but only for the range that is missing.
    print("\n=== FETCHING OR UPDATING HISTORICAL PRICES ===")
    currency = "usd"
    to_date = datetime.now().date()  # Ensure to_date is a date object
    from_date = to_date - timedelta(days=365)  # from_date is also a date object

    for network_name in CONFIG.keys():
        out_csv = f"csv/historical_prices/{network_name}_historical_prices.csv"
        coin_id = COIN_IDS.get(network_name)
        if not coin_id:
            print(f"CoinGecko ID for {network_name} not found. Skipping historical price update.")
            continue
        print(f"\nUpdating historical price CSV for {network_name} ({coin_id}) from {from_date} to {to_date}...")
        update_historical_csv(
            network_name=network_name,
            coin_id=coin_id,
            from_date=from_date,
            to_date=to_date,
            csv_path=out_csv
        )

    # 3) Compute or update USD rewards for new data points only
    print("\n=== CALCULATING/UPDATING CUMULATIVE USD REWARDS ===")
    for network_name in CONFIG.keys():
        process_rewards_for_network(network_name)

    # 4) Generate Cumulative Lending Incentives
    print("\n=== GENERATING CUMULATIVE LENDING INCENTIVES ===")
    # List all lending networks that have USD rewards CSVs
    lending_networks = ["fuse", "meter", "telos"]  # Specify the lending networks explicitly
    combined_csv_path = "csv/lending_rewards/cumulative_rewards_with_usd.csv"
    generate_combined_lending_incentives(lending_networks, combined_csv_path)

    print("\nAll done!")

if __name__ == "__main__":
    main()
