"""
Etherscan Transaction Fetcher

This script retrieves Ethereum transaction details from the Etherscan API. 
It loads API keys from an environment file, reads transaction hashes from 
'Data/WBTC-WETH.json', and makes rate-limited requests to Etherscan to 
fetch transaction details. The results are saved in 'Data/all_etherscan/WBTC-WETH_etherscan_XXX.json'.

Prerequisites:
- A '.env' file with 'ETHERSCAN_KEY'.
- A Data/WBTC-WETH.json file with extracts from the Uniswap API.

Usage:
Run as a standalone script to process and save Ethereum transaction data.
"""

import os
import json
import ratelimit
import requests
import time
import glob
import threading
from tqdm import tqdm
from dotenv import load_dotenv

# Directory path
out_directory = 'Data/all_etherscan'

# Check if the directory exists, if not, create it
if not os.path.exists(out_directory):
    os.makedirs(out_directory)

def load_env_variables(env_file):
    """Load environment variables from a given file."""
    if not load_dotenv(env_file):
        raise Exception(f"Failed to load environment file: {env_file}")

    etherscan_key = os.getenv("ETHERSCAN_KEY")
    if etherscan_key is None:
        raise Exception("ETHERSCAN_KEY not found in the environment variables.")
    return etherscan_key

def read_data(file_path):
    """Read data from a JSON file."""
    with open(file_path, "r") as file:
        return json.load(file)

def extract_transaction_hashes(tiers):
    """Extract transaction hashes from tiers data."""
    transactions = []
    for tier in tiers:
        for lp_type in tiers[tier]:
            transactions.extend(tiers[tier][lp_type])
    return [tx["id"] for tx in transactions]

# Define the rate limit decorator
@ratelimit.limits(calls=5, period=1)  # 1 requests per second
def make_api_request(api_endpoint, params):
    return requests.get(api_endpoint, params=params)

def log_failed_request(response):
    """Log failed API request."""
    timestr = time.strftime("%Y%m%d-%H%M%S")
    with open(f"Data/failed/WBTC-WETH_etherscan-{timestr}.json", "w") as file:
        json.dump(response.json(), file)

def process_transactions(transaction_hashes, etherscan_key):
    """Process each transaction and fetch details."""
    etherscan_transaction = {}
    failed_archive = load_failed_transactions()
    with tqdm(total=len(transaction_hashes), desc="Fetching transaction details") as pbar:
        for tx_id in transaction_hashes:
            txhash = tx_id.split('#')[0]
            if txhash not in failed_archive:
                tx = get_transaction_details(txhash, etherscan_key)
                if tx:  # Ensure transaction data is not None
                    etherscan_transaction[tx["hash"]] = tx
            pbar.update(1)
    return etherscan_transaction

def load_failed_transactions():
    """Load previously failed transactions."""
    file_pattern = "Data/failed/WBTC-WETH_etherscan-*.json"
    failed_archive = {}
    for file_name in glob.glob(file_pattern):
        with open(file_name, "r") as file:
            failed_archive.update(json.load(file))
    return failed_archive

def save_to_json(data, file_path):
    """Save data to a JSON file."""
    with open(file_path, "w") as file:
        json.dump(data, file)

def get_transaction_details(txhash, api_key):
    """Retrieve transaction details from Etherscan API."""
    base_url = "https://api.etherscan.io/api"
    params = {"module": "proxy", "action": "eth_getTransactionByHash", "txhash": txhash, "apikey": api_key}
    
    while True:
        try:
            response = make_api_request(base_url, params)
            if response.status_code == 200:
                return response.json()["result"]
            else:
                log_failed_request(response)
                raise Exception("Request failed with status code:", response.status_code)
        except ratelimit.RateLimitException:
            print("Rate limit exceeded. Waiting for the next rate limit window.")
            time.sleep(1)  # Wait for 1 second before retrying
            continue

def process_and_save_chunk(chunk, chunk_id, etherscan_key, semaphore=None):

    def process_and_save_logic(chunk, chunk_id, etherscan_key):
        etherscan_transaction = process_transactions(chunk, etherscan_key)
        filename = f"{out_directory}/WBTC-WETH_etherscan_{chunk_id:05d}.json"
        save_to_json(etherscan_transaction, filename)

    # Execute the block within semaphore context only if semaphore is not None
    if semaphore:
        with semaphore:
            process_and_save_logic(chunk, chunk_id, etherscan_key)
    else:
        # If no semaphore, just execute the logic directly
        process_and_save_logic(chunk, chunk_id, etherscan_key)

# Main execution
if __name__ == "__main__":
    env_file = 'environment/local-secrets.env'
    etherscan_key = load_env_variables(env_file)
    print("ETHERSCAN_KEY successfully loaded.")

    tiers = read_data("Data/WBTC-WETH.json")
    transaction_hashes = extract_transaction_hashes(tiers)

    chunk_size = 1000
    max_threads = 1 # Set to 1 to avoid rate limit issues; increase if rate limit is not an issue
    threads = []
    skip_until_chunk_id = 10000000

    if max_threads > 1: # Use threading
        threads = []
        semaphore = threading.Semaphore(max_threads)

        for i in range(0, len(transaction_hashes), chunk_size):
            chunk_id = i // chunk_size + 1
            if chunk_id <= skip_until_chunk_id:
                continue  # Skip this chunk

            chunk = transaction_hashes[i:i + chunk_size]
            thread = threading.Thread(target=process_and_save_chunk, args=(chunk, chunk_id, etherscan_key, semaphore))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
    else:
        # Non-threaded execution
        for i in range(0, len(transaction_hashes), chunk_size):
            chunk_id = i // chunk_size + 1
            if chunk_id <= skip_until_chunk_id:
                continue  # Skip this chunk

            chunk = transaction_hashes[i:i + chunk_size]
            process_and_save_chunk(chunk, chunk_id, etherscan_key, None)

    for thread in threads:
        thread.join()
