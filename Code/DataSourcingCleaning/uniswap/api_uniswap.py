"""
Uniswap Data Extraction Script

This script is designed for fetching data from Uniswap liquidity pools. It focuses on extracting data for specific time periods and performing basic analysis on various transaction types such as swaps, mints, and burns.

Key Functionalities:
- Configuration Loading: Uses an environment file to load configuration settings, including the desired time span for data analysis.
- Data Retrieval: Fetches data for each liquidity pool (LP) tier within the chosen time range, using the Uniswap APIs.
- Data Storage: Saves the fetched data in a structured JSON format in a specified directory.
- Transaction Analysis: Processes the data to format transactions, analyze swaps, mints, and burns, and identifies price extremes and transaction counts.

Usage:
Run the script to analyze Uniswap liquidity pool data for a selected time span. Ensure the environment file is set up correctly with the required configuration.

Note: The script is intended for users with a basic understanding of cryptocurrency transactions and Uniswap's decentralized exchange platform.
"""

# Make sure to set cwd to project path to run this script
import sys
import os
sys.path.append(os.getcwd())

import time
import math
import json
from dotenv import load_dotenv
from uniswap_queries import lp_queries, uniswap_transaction_extract
from environment.time_spans import load_run_config, time_spans

# Select the desired time span for analysis
# Replace 'DEMO' with 'SPAN1' or 'SPAN2' in run_config as needed
selected_span = load_run_config('environment/run-config.env')
START = time_spans[selected_span]["start"]
END = time_spans[selected_span]["end"]

# Directory where the file will be saved
directory = "Data"
file_path = os.path.join(directory, "WBTC-WETH.json")

def fetch_tier_data(start_timestamp, end_timestamp, tiers):
    """
    Fetches data for each liquidity pool (LP) tier within the given time range.

    Args:
    start_timestamp (int): The start timestamp for data retrieval.
    end_timestamp (int): The end timestamp for data retrieval.
    tiers (dict): A dictionary to store data for different tiers.

    Returns:
    dict: A dictionary with updated data for each tier.
    """
    for tier in tiers:
        tiers[tier] = {}
        for lp_type, lp_query in lp_queries.items():
            tiers[tier][lp_type] = uniswap_transaction_extract(lp_type, lp_query, start_timestamp, end_timestamp, tier)
    return tiers


#===================================================================================================#
# Below some basic analysis to validate against https://www.geckoterminal.com/eth/pools/0xcbcdf9626bc03e24f779434178a73a0b4bad62ed
def format_transactions(transactions):
    """Converts timestamps in transactions to a readable format."""
    return [{**transaction, 'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(transaction['timestamp'])))} for transaction in transactions]

def analyze_swaps(swaps):
    """Analyzes and prints information about swap transactions."""
    latest_swap = swaps[0]
    swap_price = abs(float(latest_swap['amountUSD']) / float(latest_swap['amount0']))
    print(f"Latest Swap Timestamp: {latest_swap['timestamp']}")
    print(f"Latest Swap Price: {swap_price}")

def analyze_mints(mints):
    """Analyzes and prints information about mint transactions."""
    for mint in mints:
        width = int(mint['tickUpper']) - int(mint['tickLower'])
        mint_price = math.sqrt(abs(int(mint['tickUpper']) * int(mint['tickLower'])))
        print(f"Mint Width: {width}, Mint Price: {mint_price}")
        if mint['amount0'] != '0':
            mint_weth_price = mint_price / float(mint['amountUSD']) * 2
            print(f"Mint WETH Price: {mint_weth_price}")

def analyze_burns(burns):
    """Analyzes and prints information about burn transactions."""
    latest_burn = burns[0]
    burn_price = math.sqrt(int(latest_burn['tickUpper']) * int(latest_burn['tickLower']))
    print(f"Latest Burn Timestamp: {latest_burn['timestamp']}")
    print(f"Latest Burn Price: {burn_price}")

def find_price_extremes(swaps):
    """Finds the lowest and highest prices in swap transactions."""
    lowest_price = float('inf')
    highest_price = -float('inf')
    for swap in swaps:
        price = abs(float(swap['amountUSD']) / float(swap['amount0']))
        lowest_price = min(price, lowest_price)
        highest_price = max(price, highest_price)
    print(f"Lowest Price: {lowest_price}")
    print(f"Highest Price: {highest_price}")

def count_swap_types(swaps):
    """Counts different types of swap transactions."""
    sell_count = sum(float(swap["amount0"]) > 0 for swap in swaps)
    buy_count = sum(float(swap["amount0"]) < 0 for swap in swaps)
    zero_count = sum(float(swap["amount0"]) == 0 for swap in swaps)
    print(f"Sell Transactions: {sell_count}, Buy Transactions: {buy_count}, Zero Transactions: {zero_count}")

def run_basic_analysis():
    # Basic analysis to validate against Gecko Terminal data
    swaps = format_transactions(tiers[3000]['swaps'])
    analyze_swaps(swaps)

    print(f"Number of Mints in Tier 500: {len(tiers[500]['mints'])}")
    print(f"Number of Mints in Tier 3000: {len(tiers[3000]['mints'])}")

    mints = format_transactions(tiers[500]['mints'])
    analyze_mints(mints)

    burns = format_transactions(tiers[3000]['burns'])
    analyze_burns(burns)

    find_price_extremes(swaps)
    count_swap_types(swaps)

if __name__ == "__main__":
    # Initialize tiers for different liquidity pools
    tiers = {500: None, 3000: None}

    # Fetch data for each tier
    tiers = fetch_tier_data(START, END, tiers)

    # Check if the directory exists, if not, create it
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Save the collected tier data to a JSON file
    with open(file_path, "w") as file:
        json.dump(tiers, file)

    # run_basic_analysis()