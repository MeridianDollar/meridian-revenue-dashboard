import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

# Inject custom CSS for better aesthetics
st.markdown(
    """
    <style>
    .stApp {
        background-color: #f1f1f3; /* Light gray background */
        color: #383d51;
    }

    /* Fix dropdown label text color */
    label {
        color: black !important; /* Ensure the labels are black */
    }
    </style>
    """,
    unsafe_allow_html=True
)

###############################################################################
# HELPER FUNCTIONS
###############################################################################

def load_csv(file_path):
    """
    Loads a CSV file and parses 'date_time' as datetime.
    """
    df = pd.read_csv(file_path, parse_dates=["date_time"])
    return df

def plot_rewards_vs_date(df, network_name, category_name):
    """
    Plots 'usd_rewards' vs 'date_time'.
    """
    fig, ax = plt.subplots(figsize=(12, 6), dpi=100)
    fig.patch.set_facecolor('#ffffff')  # Figure background
    ax.set_facecolor('#ffffff')        # Axes background

    ax.plot(df["date_time"], df["usd_rewards"], marker='o', linestyle='-', color='#1f77b4',
            label=f"{category_name} on {network_name}")
    ax.set_title(f"{category_name} Over Time on {network_name}", fontsize=18, fontweight='bold', color='black')
    ax.set_xlabel("Date", fontsize=14, labelpad=10, color='black')
    ax.set_ylabel("Fees (USD)", fontsize=14, labelpad=10, color='black')

    ax.grid(color='lightgray', linestyle='--', linewidth=0.5, alpha=0.7)
    ax.tick_params(axis='x', colors='black', labelsize=12, rotation=30)
    ax.tick_params(axis='y', colors='black', labelsize=12)
    ax.legend(fontsize=12, loc='upper left', frameon=True, facecolor='#ffffff',
              edgecolor='black', labelcolor='black')

    st.pyplot(fig)

###############################################################################
# CONFIGURATION FOR EACH CATEGORY
###############################################################################

category_networks = {
    "Lending Incentives": ["Telos", "Fuse", "Meter"],
    "Mint Incentives": ["Fuse", "Telos"],
    # "Lending Fees": ["Polygon", "Avalanche", "Ethereum"],
    "Mint Staking Revenue": ["Base", "Taraxa", "Telos"], # "All Combined"
    "Staking Revenue": ["Fuse", "Meter", "Telos"],
    "Redemption Staking Revenue": ["Base", "Taraxa", "Telos"]  # "All Combined" <-- NEW
}

# CSV mappings for each category
lending_csv_files = {
    "Fuse": "csv/lending_rewards/fuse_rewards_with_usd.csv",
    "Meter": "csv/lending_rewards/meter_rewards_with_usd.csv",
    "Telos": "csv/lending_rewards/telos_rewards_with_usd.csv"
}

staking_csv_files = {
    "Fuse": "csv/staking_fees/fuse_staking_fees_with_usd.csv",
    "Meter": "csv/staking_fees/meter_staking_fees_with_usd.csv",
    "Telos": "csv/staking_fees/telos_staking_fees_with_usd.csv"
}

mint_fees_csv_files = {
    "Base": "csv/mint_fees/base_mint_fees_raw.csv",
    "Taraxa": "csv/mint_fees/taraxa_mint_fees_raw.csv",
    "Telos": "csv/mint_fees/telos_mint_fees_raw.csv",
    "All Combined": "csv/mint_fees/combined_cumulative.csv",
}

# Redemption CSV files (NEW)
redemption_csv_files = {
    "Base": "csv/redemption_fees/base_redemptions_with_usd.csv",
    "Taraxa": "csv/redemption_fees/taraxa_redemptions_with_usd.csv",
    "Telos": "csv/redemption_fees/telos_redemptions_with_usd.csv",
    "All Combined": "csv/redemption_fees/combined_redemptions_with_usd.csv"
}

# Mint Incentives CSV files mapping
mint_incentives_csv_files = {
    "Fuse": "csv/mint_rewards/fuse_lqty_issued_with_usd.csv",
    "Telos": "csv/mint_rewards/telos_lqty_issued_with_usd.csv",
    "All Combined": "csv/mint_rewards/All Combined_lqty_issued_with_usd.csv"
}

###############################################################################
# STREAMLIT APP
###############################################################################

st.title("Meridian Finance Revenue")
st.write("This dashboard displays cumulative data dynamically for revenue streams on Meridian Finance")

selected_category = st.selectbox(
    "Select a Category",
    list(category_networks.keys())  # i.e. ["Lending Incentives", "Mint Incentives", ...]
)

available_networks = category_networks[selected_category]
network_name = st.selectbox(
    f"Select a Network for {selected_category}",
    available_networks
)

# Add margin
st.markdown("<div style='margin-top: 20px;'></div>", unsafe_allow_html=True)

# --------------------------------------------------------------------------
# LENDING INCENTIVES
# --------------------------------------------------------------------------
if selected_category == "Lending Incentives":
    csv_file_path = lending_csv_files.get(network_name)
    if csv_file_path:
        try:
            data = load_csv(csv_file_path)
            # Assuming the CSV has a 'reward' column in WEI, convert to ETH or appropriate unit
            if "reward" in data.columns:
                data["reward"] = data["reward"] / 10**18  # Adjust based on your data
            plot_rewards_vs_date(data, network_name, selected_category)
            st.write("Data Preview (Scrollable and Full Width):")
            st.dataframe(data, height=400, use_container_width=True)
        except FileNotFoundError:
            st.error(f"CSV file for {network_name} not found at {csv_file_path}. Please ensure the file exists.")
        except Exception as e:
            st.error(f"An error occurred while loading the data: {e}")
    else:
        st.error(f"No CSV mapping found for network: {network_name}")

# --------------------------------------------------------------------------
# MINT INCENTIVES
# --------------------------------------------------------------------------
elif selected_category == "Mint Incentives":
    csv_file_path = mint_incentives_csv_files.get(network_name)
    if csv_file_path:
        try:
            data = load_csv(csv_file_path)
            # Assuming the CSV has 'lqty_amount' and 'usd_issued' columns
            # Rename 'usd_issued' to 'usd_rewards' for consistency
            if "usd_issued" in data.columns:
                data.rename(columns={"usd_issued": "usd_rewards"}, inplace=True)
            if "lqty_amount" in data.columns:
                data.rename(columns={"lqty_amount": "lqty_issued"}, inplace=True)
            plot_rewards_vs_date(data, network_name, selected_category)
            st.write("Data Preview (Scrollable and Full Width):")
            st.dataframe(data, height=400, use_container_width=True)
        except FileNotFoundError:
            st.error(f"CSV file for {network_name} not found at {csv_file_path}. Please ensure the file exists.")
        except Exception as e:
            st.error(f"An error occurred while loading the data: {e}")
    else:
        st.error(f"No CSV mapping found for network: {network_name}")

# --------------------------------------------------------------------------
# STAKING REVENUE
# --------------------------------------------------------------------------
elif selected_category == "Staking Revenue":
    csv_file_path = staking_csv_files.get(network_name)
    if csv_file_path:
        try:
            data = load_csv(csv_file_path)
            # Assuming the CSV has a 'cumulative_fees' column, rename to 'usd_rewards'
            if "cumulative_fees" in data.columns:
                data.rename(columns={"cumulative_fees": "usd_rewards"}, inplace=True)
            plot_rewards_vs_date(data, network_name, selected_category)
            st.write("Data Preview (Scrollable and Full Width):")
            st.dataframe(data, height=400, use_container_width=True)
        except FileNotFoundError:
            st.error(f"CSV file for {network_name} not found at {csv_file_path}. Please ensure the file exists.")
        except Exception as e:
            st.error(f"An error occurred while loading the data: {e}")
    else:
        st.error(f"No CSV mapping found for network: {network_name}")

# --------------------------------------------------------------------------
# MINT STAKING REVENUE
# --------------------------------------------------------------------------
elif selected_category == "Mint Staking Revenue":
    csv_file_path = mint_fees_csv_files.get(network_name)
    if csv_file_path:
        try:
            df = pd.read_csv(csv_file_path, header=None, names=["block","date_time","cumulative_mint"], parse_dates=["date_time"])
            if "cumulative_mint" in df.columns:
                df.rename(columns={"cumulative_mint": "usd_rewards"}, inplace=True)
            plot_rewards_vs_date(df, network_name, selected_category)
            st.write("Data Preview")
            st.dataframe(df, height=400, use_container_width=True)
        except FileNotFoundError:
            st.error(f"CSV file for {network_name} not found at {csv_file_path}. Please ensure the file exists.")
        except Exception as e:
            st.error(f"An error occurred while loading the data: {e}")
    else:
        st.error(f"No CSV mapping found for network: {network_name}")

# --------------------------------------------------------------------------
# REDEMPTION STAKING REVENUE (NEW)
# --------------------------------------------------------------------------
elif selected_category == "Redemption Staking Revenue":
    csv_file_path = redemption_csv_files.get(network_name)
    if csv_file_path:
        try:
            data = load_csv(csv_file_path)  # This CSV has 'date_time', 'eth_amount', 'usd_redemptions'
    
            # Rename 'usd_redemptions' -> 'usd_rewards' for plotting
            if "usd_redemptions" in data.columns:
                data.rename(columns={"usd_redemptions": "usd_rewards"}, inplace=True)
    
            plot_rewards_vs_date(data, network_name, selected_category)
    
            # Show data
            st.write("Data Preview (Scrollable and Full Width):")
            st.dataframe(data, height=400, use_container_width=True)
        except FileNotFoundError:
            st.error(f"CSV file for {network_name} not found at {csv_file_path}. Please ensure the file exists.")
        except Exception as e:
            st.error(f"An error occurred while loading the data: {e}")
    else:
        st.error(f"No CSV mapping found for network: {network_name}")

# ---------------------------------------------------
