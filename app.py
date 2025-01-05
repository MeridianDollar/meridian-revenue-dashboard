import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

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

# Function to load data from CSV
def load_csv(file_path):
    """
    Load the CSV data and return as a pandas DataFrame.
    """
    return pd.read_csv(file_path, parse_dates=["date_time"])

# Function to plot data
def plot_rewards_vs_date(df, network_name, category_name):
    """
    Plots usd_rewards vs date_time.
    """
    fig, ax = plt.subplots(figsize=(12, 6), dpi=100)
    fig.patch.set_facecolor('#ffffff')  # Figure background
    ax.set_facecolor('#ffffff')        # Axes background

    ax.plot(df["date_time"], df["usd_rewards"], marker='o', linestyle='-', color='#1f77b4',
            label=f"Rewards on {network_name} (USD)")
    ax.set_title(f"{category_name} Over Time on {network_name}", fontsize=18, fontweight='bold', color='black')
    ax.set_xlabel("Date", fontsize=14, labelpad=10, color='black')
    ax.set_ylabel("Rewards (USD)", fontsize=14, labelpad=10, color='black')

    ax.grid(color='lightgray', linestyle='--', linewidth=0.5, alpha=0.7)
    ax.tick_params(axis='x', colors='black', labelsize=12)
    ax.tick_params(axis='y', colors='black', labelsize=12)
    ax.legend(fontsize=12, loc='upper left', frameon=True, facecolor='#ffffff', edgecolor='black', labelcolor='black')

    st.pyplot(fig)


# Example Data for Other Categories
blocks_list = np.arange(0, 100, 10).tolist()
category_data = {
    "Lending Incentives": None,  # Handled by CSV data
    "Lending Fees": [np.sqrt(block + 1) * 8 for block in blocks_list],
    "Mint Incentives": [np.sin(block / 10) * 20 + 30 for block in blocks_list],
    "Mint Fees": [np.log(block + 1) * 5 for block in blocks_list],
    "Staking Revenue": None  # We'll now load from CSV instead of simulated data
}

category_networks = {
    "Lending Incentives": ["Telos", "Fuse", "Meter"],
    "Lending Fees": ["Polygon", "Avalanche", "Ethereum"],
    "Mint Incentives": ["Solana", "Binance Smart Chain", "Telos"],
    "Mint Fees": ["Ethereum", "Fuse"],
    # Updated for your actual staking networks:
    "Staking Revenue": ["Fuse", "Meter", "Telos"],
}

# Network files for Lending Incentives
lending_csv_files = {
    "Fuse": "csv/lending_rewards/fuse_rewards_with_usd.csv",
    "Meter": "csv/lending_rewards/meter_rewards_with_usd.csv",
    "Telos": "csv/lending_rewards/telos_rewards_with_usd.csv"
}

# Network files for Staking Revenue
staking_csv_files = {
    # Adjust these paths/names to match where your script outputs the final USD CSV
    "Fuse": "csv/staking_fees/fuse_staking_fees_with_usd.csv",
    "Meter": "csv/staking_fees/meter_staking_fees_with_usd.csv",
    "Telos": "csv/staking_fees/telos_staking_fees_with_usd.csv"
}

# Streamlit App
st.title("Meridian Finance Revenue")
st.write("This dashboard displays cumulative data dynamically for different categories and networks.")

# Dropdown for category selection
selected_category = st.selectbox(
    "Select a Category",
    ["Lending Incentives", "Lending Fees", "Mint Incentives", "Mint Fees", "Staking Revenue"]
)

# Populate the network dropdown based on the selected category
available_networks = category_networks[selected_category]
network_name = st.selectbox(
    f"Select a Network for {selected_category}",
    available_networks
)

# Add margin between dropdowns and chart
st.markdown("<div style='margin-top: 20px;'></div>", unsafe_allow_html=True)

# -------------------------------------------------
# LENDING INCENTIVES
# -------------------------------------------------
if selected_category == "Lending Incentives":
    csv_file_path = lending_csv_files[network_name]
    data = load_csv(csv_file_path)

    # This is specific to your lending CSV structure, which has "reward" in raw token units:
    data["reward"] = data["reward"] / 10**18

    plot_rewards_vs_date(data, network_name, selected_category)

    # Display a scrollable, full-width preview
    st.write("Data Preview (Scrollable and Full Width):")
    st.dataframe(data, height=400, use_container_width=True)

# -------------------------------------------------
# STAKING REVENUE
# -------------------------------------------------
elif selected_category == "Staking Revenue":
    csv_file_path = staking_csv_files[network_name]
    data = load_csv(csv_file_path)
    # In your staking CSV, columns are likely: block, date_time, cumulative_fees
    # so rename "cumulative_fees" → "usd_rewards" for consistency:
    data.rename(columns={"cumulative_fees": "usd_rewards"}, inplace=True)

    plot_rewards_vs_date(data, network_name, selected_category)

    st.write("Data Preview (Scrollable and Full Width):")
    st.dataframe(data, height=400, use_container_width=True)

# -------------------------------------------------
# OTHER CATEGORIES (SIMULATED DATA)
# -------------------------------------------------
else:
    rewards_list = category_data[selected_category]

    def plot_cumulative_rewards(blocks_list, rewards_list, category_name, network_name):
        fig, ax = plt.subplots(figsize=(12, 6), dpi=100)
        fig.patch.set_facecolor('#ffffff')  # Figure background
        ax.set_facecolor('#ffffff')         # Axes background

        ax.plot(blocks_list, rewards_list, marker='o', linestyle='-', color='#1f77b4',
                label=f'{category_name} on {network_name}')
        ax.set_title(f"Cumulative {category_name} Over Time on {network_name}", fontsize=18, fontweight='bold', color='black')
        ax.set_xlabel("Block Number", fontsize=14, labelpad=10, color='black')
        ax.set_ylabel(f"Cumulative {category_name}", fontsize=14, labelpad=10, color='black')

        ax.grid(color='lightgray', linestyle='--', linewidth=0.5, alpha=0.7)
        ax.tick_params(axis='x', colors='black', labelsize=12)
        ax.tick_params(axis='y', colors='black', labelsize=12)
        ax.legend(fontsize=12, loc='upper left', frameon=True, facecolor='#ffffff', edgecolor='black', labelcolor='black')

        st.pyplot(fig)

    plot_cumulative_rewards(blocks_list, rewards_list, selected_category, network_name)
