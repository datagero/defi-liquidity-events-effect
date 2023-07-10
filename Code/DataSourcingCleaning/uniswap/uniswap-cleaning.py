import json
import pandas as pd

filepath = "Data/WBTC-WETH.json"

# Open the JSON file
with open(filepath) as json_file:
    # Load the data into a Python dictionary
    data = json.load(json_file)

# List of transaction types
transaction_types = ['swaps', 'mints', 'burns']

# Initialize an empty DataFrame
all_transactions_df = pd.DataFrame()

# Iterate over each transaction type
for transaction_type in transaction_types:
    # Initialize an empty list to store the transactions of this type
    transaction_data = []

    # Iterate over each pool
    for pool, pool_data in data.items():
        # Check if this type of transaction exists for this pool
        if transaction_type in pool_data:
            for transaction in pool_data[transaction_type]:
                # Add the pool information to the transaction data
                transaction['pool'] = pool
            # Extend the transaction data list
            transaction_data.extend(pool_data[transaction_type])

    # Create a DataFrame from the list
    transaction_df = pd.json_normalize(transaction_data)
    # Add the transaction type as a new column
    transaction_df['transaction_type'] = transaction_type
    
    # Append this DataFrame to the combined DataFrame
    all_transactions_df = pd.concat([all_transactions_df, transaction_df], ignore_index=True)

# Save the DataFrame to a CSV file
all_transactions_df.to_csv('Data/cleansed/uniswap.csv', index=False)
