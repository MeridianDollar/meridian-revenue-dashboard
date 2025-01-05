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
    "fuse": "fuse-network-token",
    "meter": "meter",
    "telos": "telos"
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
# 1) FETCH HISTORICAL PRICES (CoinGecko or Hardcoded)
###############################################################################


def generate_fixed_telos_prices(from_date, to_date):
    """
    Creates a list of [Date, 1.0] from from_date to to_date (daily).
    This is used for Telos where price is pinned to $1.
    """
    day_count = (to_date - from_date).days + 1
    # Generate a day-by-day list
    data = []
    current_day = from_date
    for _ in range(day_count):
        date_str = current_day.strftime('%Y-%m-%d')
        data.append([date_str, 1.0])  # Price always 1
        current_day += timedelta(days=1)
    return data


def save_to_csv(filename, data):
    """
    Saves a list of [Date, Price] rows to a CSV file.
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Date', 'Price'])  # Header row
        writer.writerows(data)

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
        for row in reader:
            # Expect row to be: [block, date_time, reward]
            if len(row) < 3:
                continue
            block = int(row[0])
            date = row[1]
            reward = int(row[2])
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
        cumulative_rewards = 0
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
        cumulative_rewards += chunk_rewards

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
        price = find_closest_price(reward_date, historical_prices_df)

        # Incremental difference from the previous row
        incremental_reward = current_reward - previous_reward
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
# MAIN
###############################################################################

def main():
    """
    Orchestrates the entire flow:
    1) Fetch on-chain rewards for each network (and store in CSV).
    2) Fetch or generate historical prices for each network:
       - If network is 'telos', we skip CoinGecko and use $1 price daily.
       - Otherwise, fetch from CoinGecko.
    3) Post-process to produce/update a final CSV with cumulative USD rewards
       only for newly added blocks.
    """

    # Create directories if they don't exist
    os.makedirs("csv/historical_prices", exist_ok=True)
    os.makedirs("csv/lending_rewards", exist_ok=True)

    # 1) Fetch on-chain rewards
    print("=== FETCHING ON-CHAIN REWARDS ===")
    for network_name, network_config in CONFIG.items():
        process_network(network_name, network_config)

    # 2) Historical prices: either fetch from CoinGecko or generate fixed $1 (Telos)
    print("\n=== FETCHING OR GENERATING HISTORICAL PRICES ===")
    currency = "usd"
    to_date = datetime.now()
    from_date = to_date - timedelta(days=365)  # up to 1 year in the past

    for network_name in CONFIG.keys():
        out_csv = f"csv/historical_prices/{network_name}_historical_prices.csv"

        if network_name.lower() == "telos":
            print(f"\nNetwork {network_name} → Using a fixed price of $1 daily.")
            data = generate_fixed_telos_prices(from_date, to_date)
            save_to_csv(out_csv, data)
            print(f"Saved Telos 'prices' (always 1) to {out_csv}")
        else:
            coin_id = COIN_IDS[network_name]
            print(f"\nFetching data for {network_name} ({coin_id}) from CoinGecko...")
            try:
                prices = fetch_historical_prices(coin_id, currency, from_date, to_date)
                # Convert to [Date, Price]
                formatted_data = [
                    [datetime.utcfromtimestamp(p[0] / 1000).strftime('%Y-%m-%d'), p[1]]
                    for p in prices
                ]
                save_to_csv(out_csv, formatted_data)
                print(f"Saved historical prices to {out_csv}")
            except Exception as e:
                print(f"Error while fetching prices for {network_name}: {e}")

    # 3) Compute or update USD rewards for new data points only
    print("\n=== CALCULATING/UPDATING CUMULATIVE USD REWARDS ===")
    for network_name in CONFIG.keys():
        process_rewards_for_network(network_name)


if __name__ == "__main__":
    main()
    