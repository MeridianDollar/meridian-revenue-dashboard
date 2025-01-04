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
            label=f"Rewards on {network_name} (usd)")
    ax.set_title(f"{category_name} Over Time on {network_name}", fontsize=18, fontweight='bold', color='black')
    ax.set_xlabel("Date", fontsize=14, labelpad=10, color='black')
    ax.set_ylabel("Rewards (usd)", fontsize=14, labelpad=10, color='black')

    ax.grid(color='lightgray', linestyle='--', linewidth=0.5, alpha=0.7)
    ax.tick_params(axis='x', colors='black', labelsize=12)
    ax.tick_params(axis='y', colors='black', labelsize=12)
    ax.legend(fontsize=12, loc='upper left', frameon=True, facecolor='#ffffff', edgecolor='black', labelcolor='black')

    st.pyplot(fig)


# Example Data for Other Categories
blocks_list = np.arange(0, 100, 10).tolist()
category_data = {
    "Lending Incentives": None,  # This will be handled by the CSV data
    "Lending Fees": [np.sqrt(block + 1) * 8 for block in blocks_list],
    "Mint Incentives": [np.sin(block / 10) * 20 + 30 for block in blocks_list],
    "Mint Fees": [np.log(block + 1) * 5 for block in blocks_list],
    "Staking Revenue": [block ** 1.2 for block in blocks_list],
}
category_networks = {
    "Lending Incentives": ["Telos","Fuse", "Meter"],
    "Lending Fees": ["Polygon", "Avalanche", "Ethereum"],
    "Mint Incentives": ["Solana", "Binance Smart Chain", "Telos"],
    "Mint Fees": ["Ethereum", "Fuse"],
    "Staking Revenue": ["Polkadot", "Avalanche", "Solana"],
}

# Network files for Lending Incentives
lending_csv_files = {
    "Fuse": "csv/lending_rewards/fuse_rewards_with_usd.csv",
    "Meter": "csv/lending_rewards/meter_rewards_with_usd.csv",
    "Telos": "csv/lending_rewards/telos_rewards_combined.csv"
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

if selected_category == "Lending Incentives":
    # Load the CSV file for the selected network
    csv_file_path = lending_csv_files[network_name]
    data = load_csv(csv_file_path)

    # Adjust "reward" column for readability
    data["reward"] = data["reward"] / 10**18

    # Plot the USD rewards vs date
    plot_rewards_vs_date(data, network_name, selected_category)

    # Display a scrollable and full-width preview of the data after the chart
    st.write("Data Preview (Scrollable and Full Width):")
    st.dataframe(data, height=400, use_container_width=True)  # Adjust height as needed

else:
    # Use simulated data for other categories
    rewards_list = category_data[selected_category]

    # Plot the simulated data dynamically based on the selected category and network
    def plot_cumulative_rewards(blocks_list, rewards_list, category_name, network_name):
        """
        Plots the cumulative rewards vs block number for simulated data.
        """
        fig, ax = plt.subplots(figsize=(12, 6), dpi=100)
        fig.patch.set_facecolor('#ffffff')  # Figure background
        ax.set_facecolor('#ffffff')        # Axes background

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
