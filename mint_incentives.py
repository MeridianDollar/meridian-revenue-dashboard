import os
import csv
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from web3 import Web3
# Ensure you have an 'abis.py' with necessary ABI definitions or include the ABI directly
# import abis  # Uncomment if using abis.py

###############################################################################
# HELPER FUNCTIONS / ABIs
###############################################################################

# Define the ABI for the LQTY Issuance Contract
# Ensure that the ABI includes the TotalLQTYIssuedUpdated event
LQTY_ISSUANCE_ABI = json.loads("""
[
    {
        "anonymous": false,
        "inputs": [
            {
                "indexed": false,
                "internalType": "uint256",
                "name": "latestTotalRewardsIssued",
                "type": "uint256"
            }
        ],
        "name": "TotalLQTYIssuedUpdated",
        "type": "event"
    },
    {
        "inputs": [],
        "name": "issueLQTY",
        "outputs": [
            {
                "internalType": "uint256",
                "name": "",
                "type": "uint256"
            }
        ],
        "stateMutability": "nonpayable",
        "type": "function"
    }
]
""")

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
        'from': int(from_date.timestamp()),
        'to': int(to_date.timestamp())
    }
    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        raise Exception(f"CoinGecko fetch failed: {resp.status_code} {resp.text}")

    data = resp.json()
    if "prices" not in data:
        raise KeyError("No 'prices' key in CoinGecko response.")
    return data["prices"]  # list of [timestamp_millis, price]

def generate_fixed_prices(from_date, to_date, fixed_price=1.0):
    """
    Creates a list of [timestamp_millis, fixed_price], day by day from from_date to to_date.
    Useful for tokens pegged to a fixed price.
    """
    result = []
    day_count = (to_date - from_date).days + 1
    current = from_date
    while day_count > 0:
        ts_ms = int(current.timestamp() * 1000)
        result.append([ts_ms, fixed_price])
        current += timedelta(days=1)
        day_count -= 1
    return result

def maybe_generate_or_update_historical_prices_csv(coin_id, currency, csv_path):
    """
    Checks if the historical prices CSV exists. If it does, updates it with the latest data.
    If not, generates a new CSV with up to 1 year of historical data.
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=365)

    if os.path.exists(csv_path):
        print(f"[INFO] Found existing {csv_path}. Updating with latest price data...")
        df = pd.read_csv(csv_path)
        df.columns = [c.lower().strip() for c in df.columns]
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

        last_date_in_csv = df['date'].max().date()
        new_start_date = last_date_in_csv + timedelta(days=1)

        if new_start_date >= end_date.date():
            print("No new days to fetch — CSV is up to date.")
            return

        raw_data = coingecko_fetch_prices(
            coin_id,
            currency,
            datetime(new_start_date.year, new_start_date.month, new_start_date.day),
            end_date
        )

        new_rows = []
        for (ts_ms, price) in raw_data:
            date_str = datetime.utcfromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d')
            new_rows.append([date_str, price])
        df_new = pd.DataFrame(new_rows, columns=["date", "price"])
        df_new["date"] = pd.to_datetime(df_new["date"])

        updated = pd.concat([df, df_new], ignore_index=True)
        updated.drop_duplicates(subset=["date"], keep="last", inplace=True)
        updated.sort_values("date", inplace=True)

        updated.to_csv(csv_path, index=False)
        print(f"Successfully updated {csv_path} with new data.")
    else:
        print(f"[INFO] {csv_path} does not exist — creating now...")
        raw_data = coingecko_fetch_prices(coin_id, currency, start_date, end_date)
        new_rows = []
        for (ts_ms, price) in raw_data:
            date_str = datetime.utcfromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d')
            new_rows.append([date_str, price])
        df = pd.DataFrame(new_rows, columns=["date", "price"])
        df["date"] = pd.to_datetime(df["date"])
        df.to_csv(csv_path, index=False)
        print(f"Successfully created {csv_path} with historical price data.")

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
    block_date_only = block_date.normalize() if hasattr(block_date, 'normalize') else block_date
    block_date_only = block_date_only.replace(hour=0, minute=0, second=0, microsecond=0)

    diffs = (historical_prices_df["date"] - block_date_only).abs()
    idx = diffs.idxmin()
    return float(historical_prices_df.loc[idx, "price"])

###############################################################################
# 2) CONFIGURATION
###############################################################################

CONFIG = {
    "telos": {
        "rpc": "https://rpc.telos.net",
        "default_start_block": 311768194,  # Replace with actual start block
        "block_increment": 100000,
        "contracts": {
            "lqtyIssuance": "0xC573b879Aae1a74aa6c6a5226F8E2e53644D34a4",  # Replace with actual contract address
            "collateral_coin_id": "telos",
            "csv_id": "telos"
        }
    },
    "fuse": {
        "rpc": "https://rpc.fuse.io",
        "default_start_block": 27998550,  # Replace with actual start block
        "block_increment": 30000,
        "contracts": {
            "lqtyIssuance": "0x077d17F8de5F2cC47d887e2e19A66d143ad1F14d",  # Replace with actual contract address
            "collateral_coin_id": "fuse-network-token",
            "csv_id": "fuse-network-token"
        }
    },
}


CSV_FOLDER = "csv/mint_rewards"

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
# 4) PHASE 1: Collect Raw LQTY Issuance Data
###############################################################################

def raw_csv_path(network):
    """Path for the RAW LQTY issuance CSV: [block, date_time, cumulative_lqty]."""
    return os.path.join(CSV_FOLDER, f"{network}_lqty_issued_raw.csv")

def usd_csv_path(network):
    """Path for the LQTY issuance CSV with USD: [block, date_time, lqty_amount, usd_issued]."""
    return os.path.join(CSV_FOLDER, f"{network}_lqty_issued_with_usd.csv")

def load_existing_raw_csv(network):
    """
    Returns existing data from the raw CSV, or empty lists if none.
    Format: [block, date_time, cumulative_lqty]
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

def append_raw_csv(network, block_num, date_str, cumulative_lqty):
    """
    Append a single row [block, date_time, cumulative_lqty].
    """
    path = raw_csv_path(network)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([block_num, date_str, cumulative_lqty])

def fetch_issuance_logs(lqty_contract, from_block, to_block, retries=5, delay=2):
    """
    Retrieve logs for the TotalLQTYIssuedUpdated event in the given block range using get_logs.
    Includes retry logic for transient errors.
    """
    event_signature_text = "TotalLQTYIssuedUpdated(uint256)"
    event_signature_hash = Web3.keccak(text=event_signature_text).hex()

    attempt = 0
    while attempt < retries:
        try:
            logs = lqty_contract.web3.eth.get_logs({
                'fromBlock': from_block,
                'toBlock': to_block,
                'address': lqty_contract.address,
                'topics': [event_signature_hash]
            })
            # Decode logs
            decoded_logs = [lqty_contract.events.TotalLQTYIssuedUpdated().processLog(log) for log in logs]
            return decoded_logs
        except Exception as e:
            print(f"Attempt {attempt + 1} - Error fetching issuance logs: {e}")
            time.sleep(delay)
            attempt += 1
            delay *= 2  # Exponential backoff
    print(f"Failed to fetch logs after {retries} attempts.")
    return []

def parse_issuance_logs(logs):
    """
    Extract the 'latestTotalRewardsIssued' from each TotalLQTYIssuedUpdated event log
    and return the highest cumulative LQTY issued in the block range.
    """
    max_cumulative = 0.0
    for log in logs:
        try:
            cumulative_wei = log['args']['latestTotalRewardsIssued']
            cumulative_lqty = cumulative_wei / WEI
            if cumulative_lqty > max_cumulative:
                max_cumulative = cumulative_lqty
        except Exception as e:
            print(f"Error parsing issuance log: {e}")
    return max_cumulative

def process_lqty_issuance_network(network, netconf):
    """
    Phase 1: Loop through blocks in increments, fetch all TotalLQTYIssuedUpdated events,
    determine the latest cumulative LQTY issued, and store it in a "raw" CSV file.
    """
    print(f"\n=== Processing LQTY Issuance (RAW) for network: {network} ===")
    w3 = setup_web3(netconf["rpc"])
    lqty_issuance_addr = netconf["contracts"]["lqtyIssuance"]

    # Build the contract object
    lqty_issuance = w3.eth.contract(
        address=Web3.toChecksumAddress(lqty_issuance_addr),
        abi=LQTY_ISSUANCE_ABI
    )

    # Load last synced block + last cumulative LQTY
    blocks_list, dates_list, cumulatives_list = load_existing_raw_csv(network)
    if len(blocks_list) == 0:
        last_synced_block = netconf["default_start_block"]
        cumulative_lqty = 0.0
    else:
        last_synced_block = blocks_list[-1] + 1
        cumulative_lqty = cumulatives_list[-1]

    # Current chain tip
    current_block = w3.eth.block_number
    block_increment = netconf["block_increment"]

    while last_synced_block <= current_block:
        to_block = min(last_synced_block + block_increment - 1, current_block)
        print(f"  Processing blocks {last_synced_block} → {to_block} ...")

        # Fetch Issuance logs in this range
        logs = fetch_issuance_logs(lqty_issuance, last_synced_block, to_block)

        if logs:
            # Get the maximum cumulative LQTY issued in this block range
            max_cumulative = parse_issuance_logs(logs)
            if max_cumulative > cumulative_lqty:
                cumulative_lqty = max_cumulative
        else:
            # No new issuance in this block range
            pass

        # Mark CSV row by the `to_block`'s timestamp (converted to string)
        try:
            block_dt = get_block_datetime(w3, to_block)
            block_dt_str = block_dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print(f"Error fetching block datetime for block {to_block}: {e}")
            block_dt_str = "Unknown"

        append_raw_csv(network, to_block, block_dt_str, cumulative_lqty)

        last_synced_block = to_block + 1
        current_block = w3.eth.block_number  # Update if chain advanced

    print(f"Finished collecting raw LQTY issuance data for {network}.")

###############################################################################
# 5) PHASE 2: Convert Raw LQTY Issuance to USD
###############################################################################

def load_raw_lqty_issuance_csv(network):
    """
    Load the raw LQTY issuance CSV: [block, date_time, cumulative_lqty].
    Return a DataFrame with columns: block, date_time, lqty_amount.
    """
    path = raw_csv_path(network)
    if not os.path.exists(path):
        raise FileNotFoundError(f"No raw CSV found for {network}: {path}")

    df = pd.read_csv(path, header=None, names=["block", "date_time", "lqty_amount"])
    df["date_time"] = pd.to_datetime(df["date_time"], format="%Y-%m-%d %H:%M:%S")
    df["lqty_amount"] = pd.to_numeric(df["lqty_amount"], errors="coerce")
    if df["lqty_amount"].isna().any():
        raise ValueError("Invalid numeric lqty_amount in raw CSV.")
    return df

def load_existing_usd_csv(network):
    """
    Load the existing "with USD" CSV if it exists:
    columns: [block, date_time, lqty_amount, usd_issued]
    """
    path = usd_csv_path(network)
    if not os.path.exists(path):
        return None

    df = pd.read_csv(path)
    if "block" not in df.columns or "usd_issued" not in df.columns:
        raise ValueError(f"{path} missing required columns (block, usd_issued).")
    return df

def calculate_new_usd_rows(new_df, historical_prices_df, starting_lqty, starting_usd):
    """
    new_df has columns: [block, date_time, lqty_amount].
    We'll only convert the incremental difference (lqty_amount - prev_amount) to USD
    using the historical price for that date. Accumulate into 'usd_issued'.
    """
    cumulative_usd = starting_usd
    prev_lqty_amount = starting_lqty
    usd_list = []

    for _, row in new_df.iterrows():
        dt = row["date_time"]
        price = find_closest_price(dt, historical_prices_df)  # LQTY → USD price
        current_lqty = row["lqty_amount"]

        # Incremental difference
        lqty_increment = current_lqty - prev_lqty_amount
        if lqty_increment < 0:
            print(f"Warning: Negative LQTY increment detected at block {row['block']}. Skipping.")
            lqty_increment = 0

        chunk_usd_value = lqty_increment * price

        cumulative_usd += chunk_usd_value
        usd_list.append(cumulative_usd)

        prev_lqty_amount = current_lqty

    new_df["usd_issued"] = usd_list
    return new_df

def process_lqty_issuance_usd(network, netconf):
    """
    Phase 2: Convert raw LQTY issuance (LQTY) → USD using historical prices.
    Partial update only for new blocks.
    """
    # 1) Load raw CSV
    raw_df = load_raw_lqty_issuance_csv(network)

    # 2) Ensure we have a historical prices CSV for LQTY
    coin_id = netconf["contracts"]["collateral_coin_id"]
    csv_id = netconf["contracts"]["csv_id"]
    hist_prices_csv = f"csv/historical_prices/{csv_id}_historical_prices.csv"

    maybe_generate_or_update_historical_prices_csv(coin_id, "usd", hist_prices_csv)

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
            starting_lqty=0.0,
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
        starting_lqty = last_row["lqty_amount"]
        starting_usd = last_row["usd_issued"]

        # Compute partial
        new_rows_df = calculate_new_usd_rows(new_df, historical_prices_df, starting_lqty, starting_usd)

        combined = pd.concat([existing_usd_df, new_rows_df], ignore_index=True)
        combined.sort_values(by="block", inplace=True)
        combined.to_csv(usd_path, index=False)
        print(f"Appended new data to {usd_path}.")

###############################################################################
# MAIN
###############################################################################

def main():
    """
    1) Phase 1: Collect raw LQTY issuance data for each network.
    2) Phase 2: Convert them to USD in a partial post-processing step.
    """
    os.makedirs(CSV_FOLDER, exist_ok=True)
    os.makedirs("csv/historical_prices", exist_ok=True)

    # Phase 1
    print("=== PHASE 1: Collect Raw LQTY Issuance Data ===")
    for network, netconf in CONFIG.items():
        process_lqty_issuance_network(network, netconf)

    # Phase 2
    print("\n=== PHASE 2: Convert LQTY Issuance to USD ===")
    for network, netconf in CONFIG.items():
        process_lqty_issuance_usd(network, netconf)

    print("\nAll done!")

if __name__ == "__main__":
    main()
