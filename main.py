import os
import csv
import time
from datetime import datetime
import matplotlib.pyplot as plt
from web3 import Web3
import json

############################################
# CONFIGURATION
############################################

# Load the configuration file
CONFIG_FILE = "config.json"  # Ensure this file exists in the same directory

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

def load_config(config_file):
    with open(config_file, "r") as f:
        return json.load(f)


############################################
# HELPER FUNCTIONS
############################################

def setup_web3(rpc_urls):
    """
    Sets up a Web3 instance using the first working RPC URL.
    """
    for rpc in rpc_urls:
        web3 = Web3(Web3.HTTPProvider(rpc))
        if web3.isConnected():
            return web3
    raise ConnectionError(f"Could not connect to any RPC URL: {rpc_urls}")


def load_existing_data(csv_file):
    """
    Reads existing CSV data for (blockNumber, date, cumulativeRewards).
    Returns three lists: blocks_list, dates_list, rewards_list.
    If the file doesn't exist or is empty, returns empty lists.
    """
    if not os.path.isfile(csv_file):
        return [], [], []

    blocks_list = []
    dates_list = []
    rewards_list = []

    with open(csv_file, "r", newline="") as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row
        for row in reader:
            block = int(row[0])
            date = row[1]
            reward = int(row[2])
            blocks_list.append(block)
            dates_list.append(date)
            rewards_list.append(reward)

    return blocks_list, dates_list, rewards_list


def append_data_to_csv(csv_file, block_num, date, cum_rewards):
    """
    Appends a single row [block_num, date, cum_rewards] to the CSV.
    """
    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([block_num, date, cum_rewards])


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


############################################
# MAIN LOGIC
############################################

def process_controller(network_name, version_label, controller_config, rpc_urls):
    """
    Processes a single incentives controller (whether it's named "v1", "v2", or no version).
    """
    if version_label:
        print(f"\nProcessing {network_name.upper()} - {version_label.upper()}...")
        csv_file = f"csv/lending_rewards/{network_name}_{version_label}_rewards.csv"
    else:
        # Single version (e.g., Taiko, Fuse, Meter without subkeys)
        print(f"\nProcessing {network_name.upper()}...")
        csv_file = f"csv/lending_rewards/{network_name}_rewards.csv"

    contract_address = controller_config["address"]
    default_start_block = controller_config["default_start_block"]
    chunk_size = controller_config.get("chunk_size", 100_000)  # Default to 100,000 if not in config

    # Setup Web3 and contract
    web3 = setup_web3(rpc_urls)
    contract = web3.eth.contract(address=contract_address, abi=contract_abi)
    event_signature = web3.keccak(text="RewardsAccrued(address,uint256)").hex()

    # Load existing data
    blocks_list, dates_list, rewards_list = load_existing_data(csv_file)

    # Determine the starting block and cumulative rewards
    if len(blocks_list) == 0:
        current_block = default_start_block
        cumulative_rewards = 0
    else:
        current_block = blocks_list[-1] + 1
        cumulative_rewards = rewards_list[-1]

    latest_block = web3.eth.blockNumber
    if current_block > latest_block:
        print(f"No new blocks to process for {network_name.upper()} - {version_label.upper() if version_label else ''}.")
        return

    print(f"Fetching data from block {current_block} to {latest_block}...")

    while current_block <= latest_block:
        end_block = min(current_block + chunk_size - 1, latest_block)
        print(f"  Processing blocks [{current_block} -> {end_block}]...")

        # Fetch rewards in range
        chunk_rewards = fetch_rewards_in_range(web3, contract, event_signature, current_block, end_block)

        # Update cumulative rewards
        cumulative_rewards += chunk_rewards

        # Fetch block date
        date = fetch_block_date(web3, end_block)

        # Append to CSV
        blocks_list.append(end_block)
        dates_list.append(date)
        rewards_list.append(cumulative_rewards)
        append_data_to_csv(csv_file, end_block, date, cumulative_rewards)

        current_block = end_block + 1
        time.sleep(0.2)  # Optional: avoid overloading the RPC node

    print(f"Finished processing {network_name.upper()} - {version_label.upper() if version_label else ''}.")


def process_network(network_name, network_config):
    """
    Processes all incentives controllers for a given network. 
    Supports both single and multiple "versions" of the incentives controller.
    """
    controllers = network_config["incentives_controller"]
    rpc_urls = network_config["rpcs"]

    # If there's a top-level "address" key, treat it as a single version
    if "address" in controllers and "default_start_block" in controllers:
        # Process as a single incentives controller (no version sub-keys)
        process_controller(network_name, None, controllers, rpc_urls)
    else:
        # Process multiple versions (e.g., "v1", "v2", etc.)
        for version, controller_config in controllers.items():
            process_controller(network_name, version, controller_config, rpc_urls)

def main():
    # Load the configuration
    config = load_config(CONFIG_FILE)

    # Process each network
    for network_name, network_config in config.items():
        process_network(network_name, network_config)


if __name__ == "__main__":
    main()
