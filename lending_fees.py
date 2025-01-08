import os
import csv
import time
import requests
import pandas as pd
from datetime import datetime
from web3 import Web3
from web3.exceptions import LogTopicError
from datetime import datetime, timedelta

###############################################################################
# ABIs & EVENT SIGNATURES
###############################################################################

# Minimal ERC20 ABI, just for balanceOf (adjust if needed)
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function"
    }
]

# If your Aave V2–fork deposit event differs, update accordingly
# For example, in Aave V2, the event might look like:
#   event Deposit(address indexed reserve, address user, uint256 amount, ...)
DEPOSIT_EVENT_ABI = [
    {
      "anonymous": False,
      "inputs": [
        {"indexed": True, "name": "reserve", "type": "address"},
        {"indexed": True, "name": "user",    "type": "address"},
        {"indexed": False,"name": "amount",  "type": "uint256"},
        # ... any other fields
      ],
      "name": "Deposit",
      "type": "event"
    }
]

###############################################################################
# COINGECKO PRICE FETCH
###############################################################################

def fetch_coingecko_history_range(coin_id, vs_currency, from_ts, to_ts):
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
    params = {
        'vs_currency': vs_currency,
        'from': from_ts,
        'to': to_ts
    }
    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        raise Exception(f"[ERROR] CoinGecko fetch failed: {resp.status_code} {resp.text}")
    data = resp.json()
    if "prices" not in data:
        raise KeyError("[ERROR] No 'prices' key in response.")
    return data["prices"]

def fetch_daily_prices(coin_id, vs_currency, from_date, to_date):
    from_ts = int(datetime(from_date.year, from_date.month, from_date.day).timestamp())
    to_ts   = int(datetime(to_date.year, to_date.month, to_date.day, 23, 59, 59).timestamp())
    raw_prices = fetch_coingecko_history_range(coin_id, vs_currency, from_ts, to_ts)
    daily_data = {}
    for p in raw_prices:
        dt_utc = datetime.utcfromtimestamp(p[0] / 1000.0)
        date_str = dt_utc.strftime("%Y-%m-%d")
        daily_data[date_str] = p[1]
    # Return sorted list
    sorted_dates = sorted(daily_data.keys())
    return [[d, daily_data[d]] for d in sorted_dates]

def ensure_historical_csv(coin_id, csv_folder="csv/historical_prices", vs_currency="usd", lookback_days=730):
    os.makedirs(csv_folder, exist_ok=True)
    csv_path = os.path.join(csv_folder, f"{coin_id}_historical_prices.csv")
    today = datetime.utcnow().date()

    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        if not all(col in df.columns for col in ["date", "price"]):
            raise ValueError(f"[ERROR] {csv_path} must have 'date' and 'price' columns.")
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors='coerce')
        df.dropna(subset=["date"], inplace=True)
        df.sort_values("date", inplace=True)
        df.drop_duplicates(subset=["date"], keep="last", inplace=True)
        earliest_date_in_csv = df["date"].min().date()
        latest_date_in_csv = df["date"].max().date()
    else:
        df = pd.DataFrame(columns=["date", "price"])
        earliest_date_in_csv = None
        latest_date_in_csv = None

    earliest_needed = today - timedelta(days=lookback_days)

    # Fill older gap
    if earliest_date_in_csv is None or earliest_date_in_csv > earliest_needed:
        from_date = earliest_needed
        to_date = (earliest_date_in_csv - timedelta(days=1)) if earliest_date_in_csv else today
        if from_date <= to_date:
            print(f"[INFO] Fetching older missing data for {coin_id}: {from_date} -> {to_date}")
            older_data = fetch_daily_prices(coin_id, vs_currency, from_date, to_date)
            older_df = pd.DataFrame(older_data, columns=["date","price"])
            older_df["date"] = pd.to_datetime(older_df["date"])
            df = pd.concat([df, older_df], ignore_index=True)

    # Fill forward
    if latest_date_in_csv is None or latest_date_in_csv < today:
        from_date = (latest_date_in_csv + timedelta(days=1)) if latest_date_in_csv else earliest_needed
        to_date = today
        if from_date <= to_date:
            print(f"[INFO] Fetching new data for {coin_id}: {from_date} -> {to_date}")
            new_data = fetch_daily_prices(coin_id, vs_currency, from_date, to_date)
            new_df = pd.DataFrame(new_data, columns=["date","price"])
            new_df["date"] = pd.to_datetime(new_df["date"])
            df = pd.concat([df, new_df], ignore_index=True)

    df.sort_values("date", inplace=True)
    df.drop_duplicates(subset=["date"], keep="last", inplace=True)
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    df.to_csv(csv_path, index=False)
    print(f"[DONE] {coin_id} historical CSV up-to-date at: {csv_path}")

def load_historical_prices(csv_path):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"[ERROR] Missing historical prices file: {csv_path}")
    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]
    if "date" not in df.columns or "price" not in df.columns:
        raise KeyError(f"[ERROR] Missing 'date' or 'price' columns in {csv_path}")
    df["date"] = pd.to_datetime(df["date"])
    if df["date"].isna().any():
        raise ValueError(f"[ERROR] Invalid date format in {csv_path}")
    return df

def find_closest_price(block_date, historical_prices_df):
    """
    Finds the closest date in historical_prices_df to block_date (day-level).
    Returns the corresponding price (float).
    """
    block_date = block_date.replace(hour=0, minute=0, second=0, microsecond=0)
    diffs = (historical_prices_df["date"] - block_date).abs()
    idx = diffs.idxmin()
    return float(historical_prices_df.loc[idx, "price"])

###############################################################################
# CONFIG
###############################################################################

CONFIG = {
    "telos": {
        "rpc": "https://rpc.telos.net",
        "default_start_block": 322110975,
        "block_increment": 100000,
        "contracts": {
            "oTokens": {
                "0xa55E6dC5aEC7D16793aEfE29DB74C9EED888103e": "telos",
                "0x776ADcF4E1c1C252FA783034fd1682C214Da23d4": "staked-tlos",
                "0x00cb290CA9D475506300a60D9e2A775e730b3323": "ethereum",
                "0x2E87E434662fFEBA007CA3f6375B20d38a7354d3": "bitcoin",
                "0x776ADcF4E1c1C252FA783034fd1682C214Da23d4": "staked-tlos",
                "0xdd417E6f46e7247628FA26EFaf28a10eF5E960a8": "usd-coin",
                "0x24b376800dd8F589d92Ba0c5Da099Dcdaa44Ef33": "usd-coin",
                "0x06D8a9CD225c6Ba0e60166C2e7C2c89509892Ccc": "usd-coin",
            }
        },
        "treasury_address": "0x9892F867F0E3d54cf9EdA66Cf5886bd84D973e2f",
        "mst_fee_holder": "0x873415F6633A0C42b8717bC898354638F52b13f3"
    },
    "fuse": {
        "rpc": "https://rpc.fuse.io",
        "default_start_block": 28845442,
        "block_increment": 30000,
        "contracts": {
            "oTokens": {
                "0xb012458830ed5B5A699ed2cc3A29C4b102abed6a": "fuse-network-token",
                "0x61088BCdb038bBCf33D78C5C9B232Bd6810D2281": "liquid-staked-fuse",
                "0xeC3911CCa56Ad400047EC78BbD4EDc9DcE27A745": "ethereum",
                "0xe939B9607fD0821310dEf5998A05eb4147Be3423": "wrapped-steth",
                "0xa32715Cd421475CbFD9773B4234FEb37f68CeA97": "usd-coin",
                "0xcf85542F02414f4Ff8888d174B16E27393Bd0AfD": "usd-coin",
                "0x8e4eC003B88c0A00229E31d451A9FD1533266FF1": "usd-coin",
            }
        },
        "treasury_address": "0x9892F867F0E3d54cf9EdA66Cf5886bd84D973e2f",
        "mst_fee_holder": "0x873415F6633A0C42b8717bC898354638F52b13f3"
    },
    "meter": {
        "rpc": "https://rpc.meter.io",
        "default_start_block": 51192248,
        "block_increment": 25000,
        "contracts": {
            "oTokens": {
                "0xD03B8C81eCa7311FCc6CC05f21d4Bf5023016080": "meter",
                "0x9AB0F138C65B7459D3452179856B88f106e6fA5f": "meter",
                "0x041e5F588e6D56830Df499d6cCC30ebDa4AEe3f7": "ethereum",
                "0xd9000e5a0185C523B359b338c4e2B9b97B3964b1": "usd-coin",
                "0xdEb37Be007640F86ffB2D8849b2E4DDF87C4ee78": "usd-coin",
            }
        },
        "treasury_address": "0x9892F867F0E3d54cf9EdA66Cf5886bd84D973e2f",
        "mst_fee_holder": "0x873415F6633A0C42b8717bC898354638F52b13f3"
    },
    # Add more networks if needed...
}

CSV_FOLDER = "csv/otoken_fees"

###############################################################################
# WEB3 SETUP & UTILS
###############################################################################

def setup_web3(rpc_url):
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.isConnected():
        raise ConnectionError(f"[ERROR] Could not connect to {rpc_url}")
    return w3

def get_block_datetime(w3, block_num):
    block_info = w3.eth.get_block(block_num)
    return datetime.utcfromtimestamp(block_info.timestamp)

###############################################################################
# NEW PHASE 1: Per-oToken -> Track treasury + MST fee holder balances
###############################################################################

def raw_csv_path(network, token_addr):
    """
    Store data for each oToken in a separate CSV, e.g.:
      csv/otoken_fees/<network>_<tokenAddrShort>_treasury_raw.csv
    Columns: [block, date_time, treasury_cum_eth]
    (Feel free to rename `_eth` to `_tokens` for clarity.)
    """
    short_addr = token_addr[:6].lower()
    filename = f"{network}_{short_addr}_treasury_raw.csv"
    return os.path.join(CSV_FOLDER, filename)

def load_existing_raw_csv(network, token_addr):
    path = raw_csv_path(network, token_addr)
    if not os.path.exists(path):
        return [], [], []

    blocks, dates, treasuries = [], [], []
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3:
                continue
            blocks.append(int(row[0]))
            dates.append(row[1])
            treasuries.append(float(row[2]))
    return blocks, dates, treasuries

def append_raw_csv(network, token_addr, block_num, dt_str, treasury_cum):
    path = raw_csv_path(network, token_addr)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([block_num, dt_str, treasury_cum])

def get_otoken_balance_at_block(w3, netconf, token_addr, block_number):
    """
    Return the sum of treasury + MST fee holder balances at a given block.
    Divided by 1e18 to get a float value (assuming 18 decimals).
    """
    contract = w3.eth.contract(Web3.toChecksumAddress(token_addr), abi=ERC20_ABI)

    treasury_addr = netconf.get("treasury_address", "")
    fee_addr      = netconf.get("mst_fee_holder", "")

    treasury_bal_wei = contract.functions.balanceOf(treasury_addr).call(block_identifier=block_number)
    fee_bal_wei      = contract.functions.balanceOf(fee_addr).call(block_identifier=block_number)

    return (treasury_bal_wei + fee_bal_wei) / 1e18

def process_otoken(network, netconf, token_addr):
    """
    Single oToken chunk-based loop.
    For each chunk, retrieve the sum of (treasury + MST fee holder) balances
    at 'to_block', store it in a CSV as the cumulative tokens.
    """
    w3 = setup_web3(netconf["rpc"])
    blocks, dates, treasuries = load_existing_raw_csv(network, token_addr)

    if len(blocks) == 0:
        last_synced_block = netconf["default_start_block"]
    else:
        last_synced_block = blocks[-1] + 1

    current_block = w3.eth.block_number
    increment = netconf["block_increment"]

    # Resume or start from default_start_block until the latest chain head
    while last_synced_block < current_block:
        to_block = min(last_synced_block + increment, current_block)
        print(f"[{network}:{token_addr[:6]}] blocks {last_synced_block} -> {to_block}")

        # Retrieve the combined treasury+fee balance at to_block
        balance_cum = get_otoken_balance_at_block(w3, netconf, token_addr, to_block)
        dt_str = get_block_datetime(w3, to_block).strftime("%Y-%m-%d %H:%M:%S")

        append_raw_csv(network, token_addr, to_block, dt_str, balance_cum)

        last_synced_block = to_block + 1
        current_block = w3.eth.block_number

def phase1_collect_treasury_fees(network, netconf):
    """
    For each oToken in this network, run a chunk-based loop that saves
    the cumulative treasury + MST fee holder balances to a CSV.
    """
    oTokens_dict = netconf["contracts"]["oTokens"]  # {tokenAddr: coin_id}
    for token_addr in oTokens_dict.keys():
        process_otoken(network, netconf, token_addr)

###############################################################################
# PHASE 2: Convert minted tokens -> USD (one CSV per token)
###############################################################################

def raw_csv_path_usd(network, token_addr):
    """
    For Phase 2, storing columns: [block, date_time, treasury_cum_eth, treasury_cum_usd]
    """
    short_addr = token_addr[:6].lower()
    filename = f"{network}_{short_addr}_treasury_with_usd.csv"
    return os.path.join(CSV_FOLDER, filename)

def load_raw_treasury_csv(network, token_addr):
    """
    Load the Phase 1 CSV: [block, date_time, treasury_cum_eth]
    """
    path = raw_csv_path(network, token_addr)
    if not os.path.exists(path):
        raise FileNotFoundError(f"[ERROR] No raw CSV for {network}, {token_addr} at {path}")

    df = pd.read_csv(path, header=None, names=["block","date_time","treasury_cum_eth"])
    df["date_time"] = pd.to_datetime(df["date_time"], format="%Y-%m-%d %H:%M:%S")
    df["treasury_cum_eth"] = pd.to_numeric(df["treasury_cum_eth"], errors="coerce")
    if df["treasury_cum_eth"].isna().any():
        raise ValueError(f"[ERROR] Invalid numeric data in 'treasury_cum_eth' for {network}, {token_addr}")
    return df

def load_existing_usd_csv(network, token_addr):
    path = raw_csv_path_usd(network, token_addr)
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    needed = ["block","treasury_cum_eth","treasury_cum_usd"]
    for c in needed:
        if c not in df.columns:
            raise ValueError(f"[ERROR] {path} missing column {c}.")
    return df

def calculate_new_usd_rows(new_df, hist_df, prev_eth, prev_usd):
    """
    Only convert the incremental difference in 'treasury_cum_eth' to USD.
    The difference is multiplied by the closest price for that day.
    """
    usd_list = []
    old_eth = prev_eth
    cur_usd = prev_usd

    for _, row in new_df.iterrows():
        dt = row["date_time"]
        price = find_closest_price(dt, hist_df)

        new_eth = row["treasury_cum_eth"]
        eth_incr = new_eth - old_eth
        eth_incr_usd = eth_incr * price

        cur_usd += eth_incr_usd
        usd_list.append(cur_usd)
        old_eth = new_eth

    new_df["treasury_cum_usd"] = usd_list
    return new_df

def process_token_usd(network, netconf, token_addr, coin_id):
    """
    Convert minted tokens to USD for a single token.
    """
    # 1) Load raw CSV
    df_raw = load_raw_treasury_csv(network, token_addr)

    # 2) Ensure we have historical prices
    hist_path = f"csv/historical_prices/{coin_id}_historical_prices.csv"
    if not os.path.exists(hist_path):
        print(f"[WARN] Missing historical prices for {coin_id}: {hist_path}")
        return
    hist_df = load_historical_prices(hist_path)

    # 3) Check for partial or start fresh
    path_usd = raw_csv_path_usd(network, token_addr)
    existing_usd = load_existing_usd_csv(network, token_addr)

    if existing_usd is None:
        # Full
        print(f"[INFO] No existing USD file for {network}, token: {token_addr[:6]}")
        result_df = calculate_new_usd_rows(
            new_df=df_raw,
            hist_df=hist_df,
            prev_eth=0.0,
            prev_usd=0.0
        )
        result_df.to_csv(path_usd, index=False)
        print(f"[DONE] Created new USD file: {path_usd}")
    else:
        # Partial
        last_block = existing_usd["block"].max()
        print(f"[INFO] Existing USD data up to block {last_block} for {network}, token: {token_addr[:6]}")
        new_df = df_raw[df_raw["block"] > last_block].copy()
        if new_df.empty:
            print(f"[SKIP] No new rows for {network}, token: {token_addr[:6]}")
            return

        last_row = existing_usd[existing_usd["block"] == last_block].iloc[-1]
        prev_eth = last_row["treasury_cum_eth"]
        prev_usd = last_row["treasury_cum_usd"]

        new_usd_rows = calculate_new_usd_rows(
            new_df, hist_df,
            prev_eth, prev_usd
        )

        combined = pd.concat([existing_usd, new_usd_rows], ignore_index=True)
        combined.sort_values("block", inplace=True)
        combined.to_csv(path_usd, index=False)
        print(f"[DONE] Appended new treasury fee USD data to {path_usd}")

def phase2_convert_treasury_fees_usd(network, netconf):
    """
    For each token, do partial post-processing with that token's coin_id
    """
    oTokens_dict = netconf["contracts"]["oTokens"]
    for token_addr, coin_id in oTokens_dict.items():
        # 1) Ensure historical CSV is complete
        ensure_historical_csv(
            coin_id=coin_id,
            csv_folder="csv/historical_prices",  # same folder as the rest of the script
            vs_currency="usd",                   # or whichever currency needed
            lookback_days=365                    # or however far you want to go
        )

        # 2) Convert fees to USD using that token's coin_id
        print(f"[INFO] Converting fees to USD for {network}:{token_addr[:6]} using {coin_id} data...")
        process_token_usd(network, netconf, token_addr, coin_id)

###############################################################################
# MAIN
###############################################################################

def main():
    """
    Phase 1:
      For each network, for each oToken, fetch treasury+MST balances -> store cumul. in a raw CSV.
    Phase 2:
      Convert that cumulative balance to USD, using each token's coin_id.
    """
    os.makedirs(CSV_FOLDER, exist_ok=True)
    os.makedirs("csv/historical_prices", exist_ok=True)

    # Phase 1
    for network, netconf in CONFIG.items():
        phase1_collect_treasury_fees(network, netconf)

    # Phase 2
    for network, netconf in CONFIG.items():
        phase2_convert_treasury_fees_usd(network, netconf)

    print("\n[ALL DONE]")

if __name__ == "__main__":
    main()
