import pandas as pd

def append_rewards_files_with_adjustment():
    # File paths
    file_v1 = 'csv/lending_rewards/telos_rewards_with_usd_v1.csv'
    file_v2 = 'csv/lending_rewards/telos_rewards_with_usd_v2.csv'
    output_file = 'csv/lending_rewards/telos_rewards_combined.csv'
    
    # Load both files into DataFrames
    df_v1 = pd.read_csv(file_v1)
    df_v2 = pd.read_csv(file_v2)
    
    # Add 23550.80681079955 to each value in the usd_rewards column of df_v2
    adjustment_value = 23550.80681079955
    df_v2['usd_rewards'] = df_v2['usd_rewards'] + adjustment_value
    
    # Append df_v2 to df_v1
    combined_df = pd.concat([df_v1, df_v2], ignore_index=True)
    
    # Remove duplicates, if any, based on all columns
    combined_df = combined_df.drop_duplicates()
    
    # Save the combined DataFrame to a new file
    combined_df.to_csv(output_file, index=False)
    print(f"Combined rewards file saved to: {output_file}")

if __name__ == "__main__":
    append_rewards_files_with_adjustment()