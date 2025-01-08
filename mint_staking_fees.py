import os
import csv
import json
import time
import pandas as pd
from datetime import datetime
from web3 import Web3

###############################################################################
# STUBS OR IMPORTS FOR HELPER FUNCTIONS / ABIs
###############################################################################

# For demonstration, we assume your environment can provide:
#   1) w3 connections per chain
#   2) any relevant ABIs if needed
# But the only event signature we truly need here is the Transfer event.

WEI = 10**18

###############################################################################
# CONFIGURATION
###############################################################################

# Example config. Adjust the "tokenAddress" and "mintContract" per network,
# as well as default_start_block, block_increment, etc.
CONFIG = {
    "taraxa": {
        "rpc": "https://rpc.mainnet.taraxa.io",
        "default_start_block": 13309623,
        "block_increment": 25000,
        "contracts": {
            "tokenAddress": "0xC26B690773828999c2612549CC815d1F252EA15e",     # The ERC-20 token contract
            "mintContract": "0xf6Ad62cCa52a5d3c5d567303347E013c2dadec92"  # e.g. a staking or vault contract
        }
    },
    "base": {
        "rpc": "https://base.meowrpc.com",
        "default_start_block": 2096405,
        "block_increment": 30000,
        "contracts": {
            "tokenAddress": "0x5e06eA564efcB3158a85dBF0B9E017cb003ff56f",
            "mintContract": "0xfCcD02F7a964DE33032cb57746DC3B5F9319eaB7"
        }
    },
    "telos": {
        "rpc": "https://rpc.telos.net",
        "default_start_block": 311768251,
        "block_increment": 100000,
        "contracts": {
            "tokenAddress": "0x8f7D64ea96D729EF24a0F30b4526D47b80d877B9",
            "mintContract": "0xE07D7f1C1153bCebc4f772C48A8A8eed1283ecCE"
        }
    }
}

# Directory to store final CSV data
CSV_FOLDER = "csv/mint_fees"

###############################################################################
# WEB3 SETUP & UTILS
###############################################################################

def setup_web3(rpc_url):
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if rpc_url == "https://rpc.mainnet.taraxa.io":
        return w3
    if not w3.isConnected():
        raise ConnectionError(f"Could not connect to {rpc_url}")
    return w3

def get_block_datetime(w3, block_num):
    """
    Fetch block timestamp and return 'YYYY-MM-DD HH:MM:SS'.
    """
    block_data = w3.eth.get_block(block_num)
    return datetime.utcfromtimestamp(block_data.timestamp).strftime('%Y-%m-%d %H:%M:%S')

def zero_address():
    return "0x0000000000000000000000000000000000000000"

###############################################################################
# PHASE 1: Collect raw mint fees (already in USD, no conversion needed)
###############################################################################

def raw_csv_path(network):
    """
    Path for the RAW mint CSV:
    Each row => [block, date_time, cumulative_mint_fees].
    """
    return os.path.join(CSV_FOLDER, f"{network}_mint_fees_raw.csv")

def load_existing_raw_csv(network):
    """
    Returns existing data from the raw CSV, or empty arrays if none.
    Format: [block, date_time, cumulative_mint_fees]
    """
    path = raw_csv_path(network)
    if not os.path.exists(path):
        return [], [], []

    blocks, dates, cumulatives = [], [], []
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3:
                continue
            blocks.append(int(row[0]))
            dates.append(row[1])
            cumulatives.append(float(row[2]))
    return blocks, dates, cumulatives

def append_raw_csv(network, block_num, date_time, cumulative_fees):
    """
    Append a single row [block, date_time, cumulative_mint_fees].
    """
    path = raw_csv_path(network)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([block_num, date_time, cumulative_fees])

def fetch_mint_logs(w3, token_address, from_block, to_block, to_contract):
    """
    Fetch 'Transfer' event logs indicating mint from zero address -> to_contract.
    This is typically how ERC-20 mints are recorded.
    """
    transfer_event_signature = w3.keccak(text="Transfer(address,address,uint256)").hex()

    # We'll need the addresses to be padded to 32 bytes for the filter topics
    def pad_address_to_32_bytes(address):
        return '0x' + address[2:].zfill(64)

    filter_params = {
        "fromBlock": from_block,
        "toBlock": to_block,
        "address": token_address,
        "topics": [
            transfer_event_signature,
            pad_address_to_32_bytes(zero_address()),   # from == zero address
            pad_address_to_32_bytes(to_contract)       # to == mint/staking/vault contract
        ],
    }

    try:
        logs = w3.eth.get_logs(filter_params)
        return logs
    except Exception as e:
        print(f"Error fetching mint logs: {e}")
        return []

def parse_mint_logs(w3, logs):
    minted_sum = 0.0  # float
    for log in logs:
        try:
            value_int = int(log['data'], 16)
            # Convert directly to float:
            minted_sum += float(w3.fromWei(value_int, 'ether'))
        except Exception as e:
            print(f"Error parsing mint log: {e}")
    return minted_sum


def process_mint_fees_network(network, netconf):
    """
    Phase 1 (and only phase): Loop through blocks in increments,
    fetch all mint logs from 0x0 => mintContract, sum them,
    and store a cumulative total in a CSV file.
    """
    print(f"\n=== Processing mint fees for network: {network} ===")
    w3 = setup_web3(netconf["rpc"])
    token_address = Web3.toChecksumAddress(netconf["contracts"]["tokenAddress"])
    mint_contract = Web3.toChecksumAddress(netconf["contracts"]["mintContract"])

    # Load last synced block + last cumulative fees
    blocks_list, dates_list, cumulatives_list = load_existing_raw_csv(network)
    if len(blocks_list) == 0:
        last_synced_block = netconf["default_start_block"]
        cumulative_fees = 0.0
    else:
        last_synced_block = blocks_list[-1] + 1
        cumulative_fees = cumulatives_list[-1]

    # Current chain tip
    try:
        current_block = w3.eth.block_number  # Web3 v6
    except AttributeError:
        current_block = w3.eth.blockNumber   # Web3 v5

    block_increment = netconf["block_increment"]

    while last_synced_block < current_block:
        to_block = min(last_synced_block + block_increment, current_block)
        print(f"  Processing blocks {last_synced_block} → {to_block} ...")

        # Fetch mint logs in this range
        logs = fetch_mint_logs(w3, token_address, last_synced_block, to_block, mint_contract)

        # Sum minted fees in that chunk
        chunk_minted = parse_mint_logs(w3, logs)

        # Update our cumulative
        cumulative_fees += chunk_minted

        # Use the `to_block`'s timestamp
        block_date_time = get_block_datetime(w3, to_block)
        append_raw_csv(network, to_block, block_date_time, cumulative_fees)

        last_synced_block = to_block + 1
        try:
            current_block = w3.eth.block_number
        except AttributeError:
            current_block = w3.eth.blockNumber

    print(f"Finished collecting mint fees for {network}.")

###############################################################################
# MAIN
###############################################################################

def main():
    """
    1) Fetch raw mint fees (in USD) for each network.
    2) Since fees are already in dollars, we do not need a second phase.
    """
    os.makedirs(CSV_FOLDER, exist_ok=True)

    # Phase 1 (and only): Collect Mint Fees
    print("=== PHASE 1: Collect Mint Fees (already in USD) ===")
    for network, netconf in CONFIG.items():
        process_mint_fees_network(network, netconf)

    print("\nAll done!")

if __name__ == "__main__":
    main()
