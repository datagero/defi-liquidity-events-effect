import os
import json
import ratelimit
import requests
import time
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv('environment/local-secrets.env')
etherscan_key = os.getenv("ETHERSCAN_KEY")

# Read WBTC-WETH data
tiers = json.load(open("Data/WBTC-WETH.json", "r"))

# Get all transactions
transactions = []
for tier in tiers:
    for lp_type in tiers[tier]:
        transactions.extend(tiers[tier][lp_type])

# Get all Transaction Hashes
transaction_hashes = [tx["id"] for tx in transactions]

# Define the rate limit decorator
@ratelimit.limits(calls=5, period=1)  # 1 requests per second
def make_api_request(api_endpoint, params):
    response = requests.get(api_endpoint, params=params)
    # response = requests.get(url)
    return response

def get_transaction_details(transaction_id, api_key):
    base_url = "https://api.etherscan.io/api"
    # module = "proxy"
    # action = "eth_getTransactionByHash"

    # Remove the trailing '#' and everything after it
    txhash = transaction_id.split('#')[0]

    params = {
        "module": "proxy",
        "action": "eth_getTransactionByHash",
        "txhash": txhash,
        "apikey": api_key
    }

    # Construct the API request URL
    # url = f"{base_url}?module={module}&action={action}&txhash={transaction_id}&apikey={api_key}"

    while True:
        try:
            # Make the API request while respecting the rate limit
            response = make_api_request(base_url, params)

            # Check if the request was successful
            if response.status_code == 200:
                try:
                    # Convert the response to JSON format
                    data = response.json()
                    # Transactions details are available
                    result = data["result"]
                    return result
                except Exception as e:
                    # Error occurred or no results found
                    print("Error occurred or no results found.")
                    raise e
            else:
                # print("Request failed with status code:", response.status_code)
                raise Exception("Request failed with status code:", response.status_code)
        except ratelimit.RateLimitException:
            # Handle rate limit exceeded exception
            print("Rate limit exceeded. Waiting for the next rate limit window.")
            time.sleep(1)  # Wait for 1 second before retrying
            continue

# Call the function for each transaction ID
etherscan_transaction = {}
with tqdm(total=len(transaction_hashes), desc="Fetching transaction details") as pbar:
    for i, tx_id in enumerate(transaction_hashes):
        tx = get_transaction_details(tx_id, etherscan_key)
        hash = tx["hash"]
        etherscan_transaction[hash] = tx
        pbar.update(1)

# Save tiers to json file
json.dump(etherscan_transaction, open("Data/WBTC-WETH_etherscan.json", "w"))