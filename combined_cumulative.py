import os
import pandas as pd

# These are your 3 input files within "mint_fees" folder:
file_paths = [
    "csv/mint_fees/base_mint_fees_raw.csv",
    "csv/mint_fees/taraxa_mint_fees_raw.csv",
    "csv/mint_fees/telos_mint_fees_raw.csv",
]

# Output file
output_path = "csv/mint_fees/combined_cumulative.csv"

def load_and_compute_increments(csv_path):
    """
    Loads the CSV with columns:
        block, date_time, cumulative_mint
    Converts 'cumulative_mint' into 'increment' by computing
    the difference from the previous row in that file.
    Returns a DataFrame with columns:
        date_time (as datetime), increment
    """
    df = pd.read_csv(
        csv_path, 
        header=None, 
        names=["block", "date_time", "cumulative_mint"]
    )
    # Convert date_time to datetime objects
    df["date_time"] = pd.to_datetime(df["date_time"], format="%Y-%m-%d %H:%M:%S")
    df.sort_values(by="date_time", inplace=True)
    
    # Shift the cumulative column downward to compute increments
    df["prev_cum"] = df["cumulative_mint"].shift(1).fillna(0)
    df["increment"] = df["cumulative_mint"] - df["prev_cum"]
    
    # Keep only the relevant columns
    return df[["date_time", "increment"]]

def main():
    # 1) Read each file and compute increments
    all_increments = []
    for path in file_paths:
        increments_df = load_and_compute_increments(path)
        all_increments.append(increments_df)
    
    # 2) Combine all increments into a single DF
    combined = pd.concat(all_increments, ignore_index=True)
    
    # 3) Group by exact timestamp and sum increments from all files
    combined = combined.groupby("date_time", as_index=False)["increment"].sum()
    
    # 4) Sort by timestamp
    combined.sort_values(by="date_time", inplace=True)
    
    # 5) Rename 'increment' → 'cumulative_mint' and compute an overall total
    combined.rename(columns={"increment": "cumulative_mint"}, inplace=True)
    combined["cumulative_mint"] = combined["cumulative_mint"].cumsum()
    
    # 6) Save to new CSV (date_time, cumulative_mint)
    combined.to_csv(output_path, index=False)
    print(f"Combined file saved to: {output_path}")

if __name__ == "__main__":
    main()
