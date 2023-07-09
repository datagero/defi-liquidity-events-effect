import requests
import json

lp_queries = {'swaps': """
        swaps(
        where: {
            timestamp_gte: $start,   # Unix timestamp for January 1, 2022, 00:00:00
            timestamp_lt: $end     # Unix timestamp for June 30, 2022, 23:59:59   TESTING ON 5JAN 1641312000
        },
        orderBy: timestamp, orderDirection: desc,
        first: $first,
        skip: $skip
        ) {
            id
            timestamp
            amount0
            amount1
            amountUSD             
        }
    """,

    'mints':"""
        mints(where: {
            timestamp_gte: $start,
            timestamp_lt: $end
        },
        orderBy: timestamp, orderDirection: desc,
        first: $first,
        skip: $skip
        ) {
            id
            timestamp
            amount
            amount0
            amount1
            amountUSD
            tickLower
            tickUpper
        }
    """,

    'burns':"""
        burns(where: {
            timestamp_gte: $start,
            timestamp_lt: $end
        },
        orderBy: timestamp, orderDirection: desc,
        first: $first,
        skip: $skip
        ) {
            id
            timestamp
            amount
            amount0
            amount1
            amountUSD
            tickLower
            tickUpper
        }
    """
}

def uniswap_transaction_extract(lp_type, lp_query, start, end, tier):
    # Define the API endpoint
    url = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"

    # Define the GraphQL query
    query = """
    query ($first: Int!, $skip: Int!, $start: Int!, $end: Int!, $feeTier: Int!) {
        pools(where: {
        feeTier: $feeTier,
        token0: "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
        token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    }) {""" + lp_query + "}}"

   # Set the initial variables
    first = 1000  # Number of transactions per batch
    skip = 0  # Initial value for skipping

    # Initialize an empty list to store the transactions
    all_transactions = []

    while True:
        # Send the GraphQL query with variables
        response = requests.post(url, json={"query": query, "variables": {"first": first, "skip": skip, "start":start, "end": end, "feeTier": tier}})

        # Check if the request was successful
        if response.status_code == 200:
            # Convert the response to JSON format
            data = json.loads(response.text)

            # Extract the transactions from the response
            transactions = data["data"]["pools"][0][lp_type]
            all_transactions.extend(transactions)

            # If the number of fetched transactions is less than 'first', we have reached the end of the data
            if len(transactions) < first:
                break

            # Increment the 'skip' value for the next batch
            skip += first

            # Create a new query if the limit of 5000 skips has been reached
            if skip > 5000:
                print("Reached the limit of 5000 skips. Creating a new query...")
                skip = 0
                end = int(transactions[-1]['timestamp']) - 1

        else:
            print("Request failed with status code:", response.status_code)
            break

    # Process and analyze the data as needed
    # Example: Print the response data
    # print(all_transactions)
    return all_transactions

### For debugging purposes
# import datetime
# timestamps = [int(d['timestamp']) for d in all_transactions]
# max_timestamp = max(timestamps)
# min_timestamp = min(timestamps)

# first_timestamp = int(all_transactions[0]['timestamp'])
# last_timestamp = int(all_transactions[-1]['timestamp'])

# print("Maximum Timestamp:", datetime.datetime.fromtimestamp(max_timestamp))
# print("Minimum Timestamp:", datetime.datetime.fromtimestamp(min_timestamp))
# print("First Timestamp:", datetime.datetime.fromtimestamp(first_timestamp))
# print("Last Timestamp:", datetime.datetime.fromtimestamp(last_timestamp))

# Maximum Timestamp: 2022-09-27 15:04:35
# Minimum Timestamp: 2022-09-26 22:55:11
# First Timestamp: 2022-09-27 15:04:35
# Last Timestamp: 2022-09-26 22:55:11


def uniswap_highlevel():
    """
    Experimental function to query the Uniswap v3 subgraph for high-level data
    """

    # Define the API endpoint
    url = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"

    # Define the GraphQL query
    query = """
    {
    pools(where: {
        id: "0xcbcdf9626bc03e24f779434178a73a0b4bad62ed",
        token0: "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
        token1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    }) {
        id
        feeTier
        liquidity
        volumeUSD
        token0Price
        token1Price
        tick
        poolDayData(first: 1, orderBy: date, orderDirection: desc) {
        date
        }
    }
    }
    """

    # Send the GraphQL query
    response = requests.post(url, json={"query": query})

    # Check if the request was successful
    if response.status_code == 200:
        # Convert the response to JSON format
        data = json.loads(response.text)
        # Extract the pools data
        pools = data["data"]["pools"]

        # Process and analyze the data as needed
        for pool in pools:
            # Access the desired attributes
            pool_id = pool["id"]
            fee_tier = pool["feeTier"]
            liquidity = pool["liquidity"]
            volume_usd = pool["volumeUSD"]
            token0_price = pool
    return data