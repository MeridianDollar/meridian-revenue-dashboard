import os
import csv
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from web3 import Web3
from web3._utils.events import get_event_data
from eth_abi import decode_single

###############################################################################
# STUBS OR IMPORTS FOR HELPER FUNCTIONS / ABIs
###############################################################################

# Minimal ERC20 ABI (includes `decimals` function + `Transfer` event)
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "from", "type": "address"},
            {"indexed": True, "name": "to", "type": "address"},
            {"indexed": False, "name": "value", "type": "uint256"},
        ],
        "name": "Transfer",
        "type": "event",
    },
]




def stitch_files():

    df1 = pd.read_csv("csv/staking_fees/telos_staking_fees_with_usd.csv")
    df2 = pd.read_csv("csv/staking_fees/telosV2_staking_fees_with_usd.csv")

    # 1) Sort each by date_time
    df1["date_time"] = pd.to_datetime(df1["date_time"])
    df2["date_time"] = pd.to_datetime(df2["date_time"])
    df1.sort_values("date_time", inplace=True)
    df2.sort_values("date_time", inplace=True)

    # (Optional) remove overlap:
    last_date_df1 = df1["date_time"].max()
    df2 = df2[df2["date_time"] > last_date_df1]

    # 2) Shift second file’s cumulative column
    last_cumulative = df1["usd_rewards"].iloc[-1]
    df2["usd_rewards"] = df2["usd_rewards"] + last_cumulative

    # 3) Merge & re‐sort
    combined = pd.concat([df1, df2], ignore_index=True)
    combined.sort_values("date_time", inplace=True)

    # 4) Save
    combined.to_csv("stitched_file.csv", index=False)


# In a real setup, these come from helper.py or a separate file
def fetch_historical_prices(coin_id, currency, from_date, to_date):
    """
    Stub for fetching historical prices from CoinGecko.
    Replace with real logic, e.g. requests to the CoinGecko API.
    Returns a list of [timestamp_millis, price].
    """
    mid_timestamp = int((from_date.timestamp() + to_date.timestamp()) / 2 * 1000)
    return [
        [mid_timestamp, 1.23],  # Example price
    ]

def load_historical_prices(csv_path):
    """
    Load a CSV with columns: date, price (similar to your helper.py).
    """
    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]
    if "date" not in df.columns or "price" not in df.columns:
        raise KeyError(f"Missing 'date' or 'price' in {csv_path}")
    df["date"] = pd.to_datetime(df["date"])
    return df

def find_closest_price(block_date, historical_prices_df):
    """
    Finds the closest date in `historical_prices_df` to `block_date`,
    and returns the corresponding price.
    """
    historical_prices_df["time_diff"] = abs(historical_prices_df["date"] - block_date)
    idx = historical_prices_df["time_diff"].idxmin()
    return float(historical_prices_df.loc[idx, "price"])


def update_staking_fees_data(network, staking_fees, amount_usd, fee_type):
    """
    In-memory aggregator for staking fees (USD).
    """
    existing_value = staking_fees.get(network, {}).get(fee_type, 0.0)
    if not isinstance(existing_value, (int, float)):
        try:
            existing_value = float(existing_value)
        except:
            existing_value = 0.0
    new_value = existing_value + float(amount_usd)

    if network not in staking_fees:
        staking_fees[network] = {}
    staking_fees[network][fee_type] = new_value


###############################################################################
# CONFIGURATION
###############################################################################

CONFIG = {
    "telosV2": {
        "rpc": "https://rpc.telos.net",
        "default_start_block": 365471049,
        "block_increment": 100000,
        "contracts": {
            "staking_keeper": "0x873415F6633A0C42b8717bC898354638F52b13f3",
            "staking_pool":    "0x493A60387522a7573082f0f27B98d78Ca8635e43",
            "staking_reward_tokens": [
                "0x8f7D64ea96D729EF24a0F30b4526D47b80d877B9",
                "0xD102cE6A4dB07D247fcc28F366A623Df0938CA9E"
            ]
        }
    }
}

TOKEN_COINGECKO_MAP = {
    "0x0be9e53fd7edac9f859882afdda116645287c629": "fuse-network-token",
    "0x4447863cddabbf2c3dac826f042e03c91927a196": "usd-coin",
    "0xd056eff05b69b3c612bf0e7e58b3d44d6cccc731": "telos",
    "0x8f7d64ea96d729ef24a0f30b4526d47b80d877b9": "usd-coin",
    "0x228ebbee999c6a7ad74a6130e81b12f9fe237ba3": "meter"
}

CSV_FOLDER = "csv/staking_fees"

###############################################################################
# HELPER FUNCTIONS
###############################################################################

def setup_web3(rpc_url):
    """Returns a connected Web3 instance."""
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.isConnected():
        raise ConnectionError(f"Could not connect to {rpc_url}")
    return w3

def fetch_logs(w3, token_address, from_block, to_block, topics,
               from_address=None, to_address=None):
    """
    Fetch real Transfer event logs from the chain using w3.eth.get_logs().
    """
    transfer_signature = "Transfer(address,address,uint256)"
    transfer_topic = w3.keccak(text=transfer_signature).hex()

    filter_topics = [transfer_topic]

    if from_address:
        from_topic = '0x000000000000000000000000' + from_address.lower().replace('0x', '')
    else:
        from_topic = None

    if to_address:
        to_topic = '0x000000000000000000000000' + to_address.lower().replace('0x', '')
    else:
        to_topic = None

    # Transfer event topics: [topic0=TransferSig, topic1=from, topic2=to]
    filter_topics.append(from_topic)
    filter_topics.append(to_topic)

    filter_params = {
        "fromBlock": from_block,
        "toBlock":   to_block,
        "address":   token_address,
        "topics":    filter_topics,
    }

    try:
        logs = w3.eth.get_logs(filter_params)
    except Exception as e:
        print(f"Error fetching logs for {token_address} from {from_block} to {to_block}: {e}")
        return []

    return logs

def process_transfer_logs(logs, w3, token_address):
    """
    Decode the logs to find the total token amount transferred.
    """
    # Use our minimal ERC20_ABI
    try:
        contract = w3.eth.contract(
            address=Web3.toChecksumAddress(token_address),
            abi=ERC20_ABI
        )
    except Exception as e:
        print(f"Error creating contract for {token_address}: {e}")
        return 0.0

    # Try fetching decimals
    try:
        decimals = contract.functions.decimals().call()
    except Exception as e:
        print(f"Error fetching decimals for {token_address}: {e}")
        decimals = 18
        print(f"Defaulting decimals to {decimals}")

    transfer_event_sig = w3.keccak(text="Transfer(address,address,uint256)").hex()
    total_amount = 0

    for log in logs:
        try:
            if log['topics'][0].hex() != transfer_event_sig:
                continue
            # 'value' is in the log's `data` field
            value = decode_single('uint256', bytes.fromhex(log['data'][2:]))
            total_amount += value
        except Exception as e:
            print(f"Error processing log {log['logIndex']} in block {log['blockNumber']}: {e}")
            continue

    return total_amount / (10 ** decimals)

###############################################################################
# CSV UTILS
###############################################################################

def raw_csv_path(network):
    """Returns the path for the RAW staking fees CSV: only tokens, no USD."""
    return os.path.join(CSV_FOLDER, f"{network}_staking_fees_raw.csv")

def usd_csv_path(network):
    """Returns the path for the USD-based staking fees CSV."""
    return os.path.join(CSV_FOLDER, f"{network}_staking_fees_with_usd.csv")

def load_existing_raw_csv(network):
    """
    Load existing raw CSV if it exists.
    Return (blocks_list, dates_list, token_amount_list).
    """
    path = raw_csv_path(network)
    if not os.path.exists(path):
        return [], [], []

    blocks_list, dates_list, tokens_list = [], [], []
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3:
                continue
            block = int(row[0])
            date_time = row[1]
            cumulative_tokens = float(row[2])
            blocks_list.append(block)
            dates_list.append(date_time)
            tokens_list.append(cumulative_tokens)
    return blocks_list, dates_list, tokens_list

def append_raw_csv(network, block_num, date_time, cumulative_tokens):
    """
    Append a single row [block, date_time, cumulative_tokens] to the RAW CSV.
    """
    path = raw_csv_path(network)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([block_num, date_time, cumulative_tokens])

def get_block_datetime(w3, block_num):
    """Fetch block timestamp and return 'YYYY-MM-DD HH:MM:SS'."""
    block_data = w3.eth.get_block(block_num)
    return datetime.utcfromtimestamp(block_data.timestamp).strftime('%Y-%m-%d %H:%M:%S')

###############################################################################
# PHASE 1: Collect raw token amounts
###############################################################################

def process_staking_fees_network(network, network_config):
    """
    Phase 1: Collect raw on-chain token transfers (summing all reward tokens).
    Save to a CSV: [block, date_time, cumulative_token_amount].
    We do NOT fetch prices or compute USD here.
    """
    print(f"\n=== Processing staking fees (RAW) for network: {network} ===")

    w3 = setup_web3(network_config["rpc"])

    # 1) Determine last processed block from raw CSV
    blocks_list, dates_list, tokens_list = load_existing_raw_csv(network)
    if len(blocks_list) == 0:
        last_synced_block = network_config["default_start_block"]
        cumulative_tokens = 0.0
    else:
        last_synced_block = blocks_list[-1] + 1
        cumulative_tokens = tokens_list[-1]

    # 2) Current chain block
    try:
        current_block = w3.eth.block_number  # v6
    except AttributeError:
        current_block = w3.eth.blockNumber   # v5

    block_increment = network_config["block_increment"]
    from_address = network_config["contracts"]["staking_keeper"]
    to_address   = network_config["contracts"]["staking_pool"]

    # 3) Iterate in chunks, storing only token amounts
    while last_synced_block < current_block:
        to_block = min(last_synced_block + block_increment, current_block)
        print(f"  Processing blocks {last_synced_block} → {to_block} ...")

        # Sum all tokens transferred in this chunk
        chunk_token_amount = 0.0
        block_date_time = get_block_datetime(w3, to_block)

        for token_address in network_config["contracts"]["staking_reward_tokens"]:
            logs = fetch_logs(
                w3=w3,
                token_address=token_address,
                from_block=last_synced_block,
                to_block=to_block,
                topics=[],
                from_address=from_address,
                to_address=to_address
            )
            chunk_token_amount += process_transfer_logs(logs, w3, token_address)

        # Update cumulative
        cumulative_tokens += chunk_token_amount

        # Write to raw CSV
        append_raw_csv(network, to_block, block_date_time, cumulative_tokens)

        # Move forward
        last_synced_block = to_block + 1

        # Re-check tip
        try:
            current_block = w3.eth.block_number
        except AttributeError:
            current_block = w3.eth.blockNumber

    print(f"Finished collecting raw token amounts for {network}.")

###############################################################################
# PHASE 2: Partial Post-Processing to get USD
###############################################################################

def load_raw_staking_fees_csv(network):
    """
    Loads the raw staking fees CSV with columns [block, date_time, cumulative_token_amount].
    Returns a DataFrame with columns: block, date_time, token_amount
    """
    path = raw_csv_path(network)
    if not os.path.exists(path):
        raise FileNotFoundError(f"No raw CSV found for {network}: {path}")

    df = pd.read_csv(path, header=None, names=["block", "date_time", "token_amount"])
    df["date_time"] = pd.to_datetime(df["date_time"], format="%Y-%m-%d %H:%M:%S")
    df["token_amount"] = pd.to_numeric(df["token_amount"], errors="coerce")
    if df["token_amount"].isna().any():
        raise ValueError("Invalid numeric token_amount in raw CSV.")
    return df

def load_existing_usd_csv(network):
    """
    Loads the existing USD CSV if present:
    columns: [block, date_time, token_amount, usd_rewards]
    Returns DataFrame or None if doesn't exist.
    """
    path = usd_csv_path(network)
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    if "block" not in df.columns or "usd_rewards" not in df.columns:
        raise ValueError(f"{path} missing required columns (block, usd_rewards).")
    return df

def calculate_new_usd_rows(new_df, historical_prices_df, starting_token, starting_usd):
    """
    new_df has columns: [block, date_time, token_amount].
    We only convert the incremental difference in token_amount,
    multiply by the daily price, and produce a new 'usd_rewards' column.
    We'll accumulate into a final 'usd_rewards' (cumulative).
    """
    cumulative_usd = starting_usd
    prev_token_amount = starting_token
    usd_list = []

    for _, row in new_df.iterrows():
        dt = row["date_time"]
        # Convert to daily date if you want daily resolution
        # or just use dt.date() if you store daily prices only
        price = find_closest_price(dt, historical_prices_df)

        current_token_amount = row["token_amount"]
        increment = current_token_amount - prev_token_amount
        # If your real token is 18 decimals, ensure your raw CSV matched that
        # Right now we assume the raw "token_amount" is already in human units
        chunk_usd_value = increment * price

        cumulative_usd += chunk_usd_value
        usd_list.append(cumulative_usd)

        prev_token_amount = current_token_amount

    new_df["usd_rewards"] = usd_list
    return new_df

def process_staking_fees_usd(network):
    """
    Phase 2: Takes the raw CSV for `network`, merges with historical prices,
    and produces/updates a CSV with cumulative USD fees.
    """
    raw_df = load_raw_staking_fees_csv(network)
    # If you have a single token only, you'd fetch that token's coin_id, etc.
    # But here we have *multiple tokens*, so you might choose a "blended" approach
    # or store them as a single row. We assume you already combined them in raw_df.

    # We'll do daily historical prices for now. (You might store them in
    # `csv/historical_prices/NETWORK.csv`, or do it per token if needed.)
    # For brevity, we'll just load a single "dummy" CSV. Replace with your real logic:
    hist_prices_csv = os.path.join("csv", "historical_prices", f"{network}_historical_prices.csv")
    if not os.path.exists(hist_prices_csv):
        print(f"Missing historical prices CSV for {network}: {hist_prices_csv}")
        return

    historical_prices_df = load_historical_prices(hist_prices_csv)

    # See if we already have a partial USD file
    existing_usd = load_existing_usd_csv(network)
    usd_path = usd_csv_path(network)

    if existing_usd is None:
        # No existing USD file: compute from scratch
        print(f"No existing USD file for {network}, computing from scratch...")
        result_df = calculate_new_usd_rows(
            new_df=raw_df,
            historical_prices_df=historical_prices_df,
            starting_token=0.0,
            starting_usd=0.0
        )
        result_df.to_csv(usd_path, index=False)
        print(f"Saved new file to {usd_path}.")
    else:
        # We have partial data
        last_block = existing_usd["block"].max()
        print(f"Found existing USD data up to block {last_block} for {network}.")

        # Filter to only new blocks
        new_df = raw_df[raw_df["block"] > last_block].copy()
        if new_df.empty:
            print(f"No new rows for {network}. Nothing to update.")
            return

        # Get the final row from existing data to carry over cumulative
        last_row = existing_usd[existing_usd["block"] == last_block].iloc[-1]
        starting_token = last_row["token_amount"]
        starting_usd = last_row["usd_rewards"]

        # Calculate partial
        new_usd_rows = calculate_new_usd_rows(new_df, historical_prices_df, starting_token, starting_usd)

        # Merge them
        combined = pd.concat([existing_usd, new_usd_rows], ignore_index=True)
        combined.sort_values(by="block", inplace=True)
        combined.to_csv(usd_path, index=False)
        print(f"Appended new data to {usd_path}.")

###############################################################################
# MAIN
###############################################################################

def main():
    """
    1) Phase 1: fetch raw token amounts for each network (no USD).
    2) Phase 2: post-process each network to compute USD, partial updates only.
    """
    # Make sure folders exist
    os.makedirs(CSV_FOLDER, exist_ok=True)
    os.makedirs("csv/historical_prices", exist_ok=True)

    # Phase 1: Collect raw token amounts
    print("=== PHASE 1: Collect Raw Staking Fees (Tokens) ===")
    for network, netconf in CONFIG.items():
        process_staking_fees_network(network, netconf)

    # Phase 2: Convert new raw data → USD
    print("\n=== PHASE 2: Convert to USD with partial post-processing ===")
    for network in CONFIG.keys():
        process_staking_fees_usd(network)
        
    stitch_files()

    print("\nAll done!")

if __name__ == "__main__":
    main()
    
