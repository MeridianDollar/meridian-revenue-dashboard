import pandas as pd
import os

def process_telos_rewards():
    # Define the file paths
    input_file = 'csv/lending_rewards/telos_rewards.csv'
    output_file = 'csv/lending_rewards/telos_rewards_with_usd.csv'
    
    # Check if the input file exists
    if not os.path.exists(input_file):
        print(f"Input file not found: {input_file}")
        return
    
    # Read the CSV file without a header
    df = pd.read_csv(input_file, header=None, names=['block', 'date_time', 'reward'])
    
    # Ensure 'reward' is numeric
    df['reward'] = pd.to_numeric(df['reward'], errors='coerce')
    
    # Calculate 'usd_rewards' by dividing 'reward' by 1e18
    df['usd_rewards'] = df['reward'] / 1e18
    
    # Save the updated DataFrame with headers
    df.to_csv(output_file, index=False)
    print(f"Processed rewards saved to {output_file}")

if __name__ == "__main__":
    process_telos_rewards()
