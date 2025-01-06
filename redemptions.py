import os
import csv
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from web3 import Web3
import abis

###############################################################################
# HELPER FUNCTIONS / ABIs
###############################################################################

TROVE_MANAGER_ABI = abis.troveManager()
WEI = 10**18

###############################################################################
# 1) FETCH OR GENERATE HISTORICAL PRICES
###############################################################################

def coingecko_fetch_prices(coin_id, currency, from_date, to_date):
    """
    Fetches historical price data from CoinGecko's 'range' endpoint.
    Returns a list of [timestamp_millis, price].
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {
        'vs_currency': currency,
        'from': from_date.timestamp(),
        'to': to_date.timestamp()
    }
    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        raise Exception(f"CoinGecko fetch failed: {resp.status_code} {resp.text}")

    data = resp.json()
    if "prices" not in data:
        raise KeyError("No 'prices' key in CoinGecko response.")
    return data["prices"]  # list of [timestamp_millis, price]

def generate_fixed_telos_prices(from_date, to_date):
    """
    Creates a list of [timestamp_millis, 1.0], day by day from from_date to to_date.
    Telos or any other chain you want pinned to $1 can be handled here.
    """
    result = []
    day_count = (to_date - from_date).days + 1
    current = from_date
    while day_count > 0:
        # Convert date to a "timestamp_millis"
        ts_ms = int(current.timestamp() * 1000)
        result.append([ts_ms, 1.0])
        current += timedelta(days=1)
        day_count -= 1
    return result

def maybe_generate_or_update_historical_prices_csv(coin_id, currency, csv_path):
    # We'll define a cutoff of up to 1 year in the past
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)

    if os.path.exists(csv_path):
        print(f"[INFO] Found existing {csv_path}. Updating with latest price data...")
        # 1) Load existing CSV
        df = pd.read_csv(csv_path)
        df.columns = [c.lower().strip() for c in df.columns]
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # 2) Find the newest date we already have
        last_date_in_csv = df['date'].max().date()
        # We can start fetching from day after that last date
        new_start_date = last_date_in_csv + timedelta(days=1)
        
        if new_start_date >= end_date.date():
            print("No new days to fetch — CSV is up to date.")
            return
        
        # 3) Fetch new data from CoinGecko
        #    (If coin_id is 'telos', handle pinned logic, etc.)
        raw_data = coingecko_fetch_prices(coin_id, currency,
                                          datetime(new_start_date.year, new_start_date.month, new_start_date.day),
                                          end_date)
        
        # 4) Convert to [Date, Price] and append to existing DataFrame
        new_rows = []
        for (ts_ms, price) in raw_data:
            date_str = datetime.utcfromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d')
            new_rows.append([date_str, price])
        df_new = pd.DataFrame(new_rows, columns=["date", "price"])
        df_new["date"] = pd.to_datetime(df_new["date"])
        
        # 5) Concatenate, sort, and drop any duplicates
        updated = pd.concat([df, df_new], ignore_index=True)
        updated.drop_duplicates(subset=["date"], keep="last", inplace=True)
        updated.sort_values("date", inplace=True)
        
        # 6) Save the updated CSV
        updated.to_csv(csv_path, index=False)
        print(f"Successfully updated {csv_path} with new data.")
    else:
        print(f"[INFO] {csv_path} does not exist — creating now...")
        # The same logic that fetches an entire year and writes from scratch
        # (the same as maybe_generate_historical_prices_csv in the prior example).
        # ...


def load_historical_prices(csv_path):
    """
    Loads a CSV with columns: date, price.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing historical prices file: {csv_path}")

    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]
    if "date" not in df.columns or "price" not in df.columns:
        raise KeyError(f"Missing 'date' or 'price' columns in {csv_path}")

    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    if df["date"].isna().any():
        raise ValueError(f"Invalid date format in {csv_path}")
    return df

def find_closest_price(block_date, historical_prices_df):
    """
    Finds the closest date in `historical_prices_df` to `block_date` and
    returns the corresponding price (float).
    """
    # Convert block_date to just a date (ignore HH:MM:SS for daily data).
    # Or keep it as a datetime if you prefer a more precise approach.
    block_date_only = block_date.normalize() if hasattr(block_date, 'normalize') else block_date
    block_date_only = block_date_only.replace(hour=0, minute=0, second=0, microsecond=0)

    # Compute absolute difference to each row's date
    diffs = (historical_prices_df["date"] - block_date_only).abs()
    idx = diffs.idxmin()
    return float(historical_prices_df.loc[idx, "price"])


###############################################################################
# 2) CONFIGURATION
###############################################################################

CONFIG = {
    "fuse": {
        "rpc": "https://rpc.fuse.io",
        "default_start_block": 27998541,
        "block_increment": 30000,
        "contracts": {
            "troveManager": "0xCD413fC3347cE295fc5DB3099839a203d8c2E6D9",
            "collateral_coin_id": "fuse-network-token"
        }
    },
    "base": {
        "rpc": "https://base.meowrpc.com",
        "default_start_block": 2096194,
        "block_increment": 30000,
        "contracts": {
            "troveManager": "0x56a901FdF67FC52e7012eb08Cfb47308490A982C",
            "collateral_coin_id": "ethereum"
        }
    },
    "telos": {
        "rpc": "https://rpc.telos.net",
        "default_start_block": 311768153,
        "block_increment": 100000,
        "contracts": {
            "troveManager": "0xb1F92104E1Ad5Ed84592666EfB1eB52b946E6e68",
            "collateral_coin_id": "telos"
        }
    }
}

CSV_FOLDER = "csv/redemption_fees"

###############################################################################
# 3) WEB3 SETUP & UTILS
###############################################################################

def setup_web3(rpc_url):
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.isConnected():
        raise ConnectionError(f"Could not connect to {rpc_url}")
    return w3

def get_block_datetime(w3, block_num):
    """
    Fetch block timestamp and return a datetime object (UTC).
    """
    block_data = w3.eth.get_block(block_num)
    return datetime.utcfromtimestamp(block_data.timestamp)

###############################################################################
# 4) PHASE 1: Collect raw redemption fees (no USD)
###############################################################################

def raw_csv_path(network):
    """Path for the RAW redemption CSV: [block, date_time, cumulative_redemptions_eth]."""
    return os.path.join(CSV_FOLDER, f"{network}_redemptions_raw.csv")

def usd_csv_path(network):
    """Path for the redemption CSV with USD: [block, date_time, eth_amount, usd_redemptions]."""
    return os.path.join(CSV_FOLDER, f"{network}_redemptions_with_usd.csv")

def load_existing_raw_csv(network):
    """
    Returns existing data from the raw CSV, or empty lists if none.
    Format: [block, date_time, cumulative_redemptions_eth]
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

def append_raw_csv(network, block_num, date_str, cumulative_eth):
    """
    Append a single row [block, date_time, cumulative_redemptions_eth].
    """
    path = raw_csv_path(network)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([block_num, date_str, cumulative_eth])

def fetch_redemption_logs(trove_manager_contract, from_block, to_block):
    """
    Retrieve logs for the Redemption event in the given block range.
    """
    try:
        logs = trove_manager_contract.events.Redemption().getLogs(
            fromBlock=from_block,
            toBlock=to_block
        )
        return logs
    except Exception as e:
        print(f"Error fetching redemption logs: {e}")
        return []

def parse_redemption_logs(logs):
    """
    Extract the `_ETHFee` from each Redemption event log,
    convert from WEI to ETH, and return the sum of all fees in that block range.
    """
    redemption_sum = 0.0
    for log in logs:
        try:
            wei_amount = log['args']['_ETHFee']
            redemption_sum += wei_amount / WEI
        except Exception as e:
            print(f"Error parsing redemption log: {e}")
    return redemption_sum

def process_redemptions_network(network, netconf):
    """
    Phase 1: Loop through blocks in increments, fetch all Redemption events, sum them,
    and store a cumulative total in a "raw" CSV file. No USD conversion here.
    """
    print(f"\n=== Processing redemption fees (RAW) for network: {network} ===")
    w3 = setup_web3(netconf["rpc"])
    trove_manager_addr = netconf["contracts"]["troveManager"]

    # Build the contract object
    trove_manager = w3.eth.contract(
        address=Web3.toChecksumAddress(trove_manager_addr),
        abi=TROVE_MANAGER_ABI
    )

    # Load last synced block + last cumulative redemption
    blocks_list, dates_list, cumulatives_list = load_existing_raw_csv(network)
    if len(blocks_list) == 0:
        last_synced_block = netconf["default_start_block"]
        cumulative_eth = 0.0
    else:
        last_synced_block = blocks_list[-1] + 1
        cumulative_eth = cumulatives_list[-1]

    # Current chain tip
    current_block = w3.eth.block_number
    block_increment = netconf["block_increment"]

    while last_synced_block < current_block:
        to_block = min(last_synced_block + block_increment, current_block)
        print(f"  Processing blocks {last_synced_block} → {to_block} ...")

        # Fetch Redemption logs in this range
        logs = fetch_redemption_logs(trove_manager, last_synced_block, to_block)

        # Sum the ETH fees in this chunk
        chunk_eth = parse_redemption_logs(logs)

        # Update our cumulative
        cumulative_eth += chunk_eth

        # Mark CSV row by the `to_block`'s timestamp (converted to string)
        block_dt = get_block_datetime(w3, to_block)
        block_dt_str = block_dt.strftime('%Y-%m-%d %H:%M:%S')
        append_raw_csv(network, to_block, block_dt_str, cumulative_eth)

        last_synced_block = to_block + 1
        current_block = w3.eth.block_number  # update if chain advanced

    print(f"Finished collecting raw redemption fees for {network}.")

###############################################################################
# 5) PHASE 2: Convert raw redemption (ETH) → USD with partial post-processing
###############################################################################

def load_raw_redemptions_csv(network):
    """
    Load the raw redemption fees CSV: [block, date_time, cumulative_redemptions_eth].
    Return a DataFrame with columns: block, date_time, eth_amount.
    """
    path = raw_csv_path(network)
    if not os.path.exists(path):
        raise FileNotFoundError(f"No raw CSV found for {network}: {path}")

    df = pd.read_csv(path, header=None, names=["block", "date_time", "eth_amount"])
    df["date_time"] = pd.to_datetime(df["date_time"], format="%Y-%m-%d %H:%M:%S")
    df["eth_amount"] = pd.to_numeric(df["eth_amount"], errors="coerce")
    if df["eth_amount"].isna().any():
        raise ValueError("Invalid numeric eth_amount in raw CSV.")
    return df

def load_existing_usd_csv(network):
    """
    Load the existing "with USD" CSV if it exists:
    columns: [block, date_time, eth_amount, usd_redemptions]
    """
    path = usd_csv_path(network)
    if not os.path.exists(path):
        return None

    df = pd.read_csv(path)
    if "block" not in df.columns or "usd_redemptions" not in df.columns:
        raise ValueError(f"{path} missing required columns (block, usd_redemptions).")
    return df

def calculate_new_usd_rows(new_df, historical_prices_df, starting_eth, starting_usd):
    """
    new_df has columns: [block, date_time, eth_amount].
    We'll only convert the incremental difference (eth_amount - prev_amount) to USD
    using the historical price for that date. Accumulate into 'usd_redemptions'.
    """
    cumulative_usd = starting_usd
    prev_eth_amount = starting_eth
    usd_list = []

    for _, row in new_df.iterrows():
        dt = row["date_time"]
        price = find_closest_price(dt, historical_prices_df)  # e.g. ETH → USD price
        current_eth = row["eth_amount"]

        # Incremental difference
        eth_increment = current_eth - prev_eth_amount
        chunk_usd_value = eth_increment * price

        cumulative_usd += chunk_usd_value
        usd_list.append(cumulative_usd)

        prev_eth_amount = current_eth

    new_df["usd_redemptions"] = usd_list
    return new_df

def process_redemptions_usd(network, netconf):
    """
    Phase 2: Convert raw redemptions (ETH) → USD using historical prices.
    Partial update only for new blocks.
    """
    # 1) Load raw CSV
    raw_df = load_raw_redemptions_csv(network)

    # 2) Ensure we have a historical prices CSV for the underlying collateral
    coin_id = netconf["contracts"]["collateral_coin_id"]
    hist_prices_csv = f"csv/historical_prices/{coin_id}_historical_prices.csv"

    # If no CSV yet, auto-generate from CoinGecko or pinned logic
    maybe_generate_historical_prices_csv(coin_id, "usd", hist_prices_csv)

    # Now load that CSV into a DataFrame
    historical_prices_df = load_historical_prices(hist_prices_csv)

    # 3) Check for existing "with USD" file
    usd_path = usd_csv_path(network)
    existing_usd_df = load_existing_usd_csv(network)

    if existing_usd_df is None:
        print(f"No existing USD file for {network}, computing from scratch...")
        result_df = calculate_new_usd_rows(
            new_df=raw_df,
            historical_prices_df=historical_prices_df,
            starting_eth=0.0,
            starting_usd=0.0
        )
        result_df.to_csv(usd_path, index=False)
        print(f"Saved new file to {usd_path}.")
    else:
        # Partial update for new blocks only
        last_block = existing_usd_df["block"].max()
        print(f"Found existing USD data up to block {last_block} for {network}.")
        new_df = raw_df[raw_df["block"] > last_block].copy()
        if new_df.empty:
            print(f"No new rows for {network}. Nothing to update.")
            return

        # Resume from last known cumulative values
        last_row = existing_usd_df[existing_usd_df["block"] == last_block].iloc[-1]
        starting_eth = last_row["eth_amount"]
        starting_usd = last_row["usd_redemptions"]

        # Compute partial
        new_rows_df = calculate_new_usd_rows(new_df, historical_prices_df, starting_eth, starting_usd)

        combined = pd.concat([existing_usd_df, new_rows_df], ignore_index=True)
        combined.sort_values(by="block", inplace=True)
        combined.to_csv(usd_path, index=False)
        print(f"Appended new data to {usd_path}.")

###############################################################################
# MAIN
###############################################################################

def main():
    """
    1) Phase 1: Collect raw redemption fees (in ETH or underlying collateral) for each network.
    2) Phase 2: Convert them to USD in a partial post-processing step.
    """
    os.makedirs(CSV_FOLDER, exist_ok=True)
    os.makedirs("csv/historical_prices", exist_ok=True)

    # Phase 1
    print("=== PHASE 1: Collect Raw Redemption Fees (no USD) ===")
    for network, netconf in CONFIG.items():
        process_redemptions_network(network, netconf)

    # Phase 2
    print("\n=== PHASE 2: Convert Redemption Fees to USD ===")
    for network, netconf in CONFIG.items():
        process_redemptions_usd(network, netconf)

    print("\nAll done!")

if __name__ == "__main__":
    main()
