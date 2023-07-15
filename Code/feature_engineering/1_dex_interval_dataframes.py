"""
This script performs cleaning, preprocessing, merging, and analysis operations on Uniswap, Etherscan, and Binance data. It also logs information about data loss and transaction types. The inferred block intervals are saved as interim results for further analysis.

The provided code performs data processing and interval analysis on Uniswap, Etherscan, and Binance datasets. Here is a summary of the main functionalities:

1. Clean Uniswap Data: The code reads and cleans the Uniswap data, removing any additional information after the '#' symbol in the 'id' column.
2. Clean Etherscan Data: The code reads and cleans the Etherscan data.
3. Preprocess Data: The code preprocesses the merged DEX (Decentralized Exchange) data by converting timestamps to datetime format, sorting the data, converting hexadecimal block numbers to integers, and creating additional columns for analysis.
4. Clean Mint Transactions: The code reduces mint transactions on the same block, optimizing the dataset for interval analysis.
5. Infer Block Intervals: The code infers block intervals and creates interval-based dataframes. It calculates intervals based on the 'pool' column and specified shift periods. It also calculates additional intervals for the 'other' pool.
6. Save Interim Results: The code saves the intermediate results, including the reduced DEX data, block data, and interval dataframes, to separate CSV files and a pickle file.
7. Log Count of Transaction Types: The code logs the count of transaction types, providing insights into the distribution of transactions across pools and types.

The main function orchestrates these functionalities and is the entry point of the program.

"""

import os
import pickle
import pandas as pd
import numpy as np
from collections import Counter
from utils.build_intervals import calculate_intervals, calculate_other_intervals, create_interval_dataframes, reduce_mints

# Define shift periods for the intervals
SHIFT_PERIODS = range(0, 4)

def validate_block_number_order(df):
    df_sorted_timestamp = df.sort_values(by='timestamp')
    df_sorted_blocknumber = df.sort_values(by='blockNumber')
    block_number_order_same = (df_sorted_timestamp['blockNumber'].values == df_sorted_blocknumber['blockNumber'].values).all()
    assert block_number_order_same, "Sorting by timestamp and block number does not result in the same block number order"

def clean_uniswap_data(uniswap_filepath):
    df_uniswap = pd.read_csv(uniswap_filepath)
    df_uniswap['id'] = df_uniswap['id'].str.split('#').str[0]

    #TODO -> Check why we have duplicated transaction IDs with different data.
    duplicates = df_uniswap[df_uniswap['id'].duplicated()]
    df_uniswap[df_uniswap['id']==duplicates['id'].iloc[0]]
    return df_uniswap

def clean_etherscan_data(etherscan_filepath):
    df_etherscan = pd.read_csv(etherscan_filepath)
    return df_etherscan

def preprocess_data(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s').dt.tz_localize('UTC')
    df.sort_values(by='timestamp', inplace=True)
    df['blockNumber'] = df['blockNumber'].apply(lambda x: int(x, 16))
    df['size'] = df['amountUSD']
    df['width'] = df['tickUpper'] - df['tickLower']
    df['pool_price'] = np.nan
    df.loc[df['transaction_type'] == 'swaps', 'pool_price'] = df['amount1'] / df['amount0']
    return df

def clean_mint_transactions(df):
    df_reduced = reduce_mints(df[['timestamp', 'transaction_type', 'pool', 'blockNumber', 'size', 'width', 'pool_price', 'amountUSD']])
    return df_reduced

def infer_block_intervals(df_reduced):
    """
    Overall, this process involves calculating intervals based on specified columns, creating interval-based dataframes, and organizing them in a dictionary structure for further analysis and processing.

    Note -> For the blocks, the pair of reference blockNumber and pool should be unique.
    This allow us to use this pair as the index/hashid for interval_dataframes.
    This hash is then used for future reference to get the pool and reference blockNumber.
    See the generate_hash() function in build_intervals.py for more details or to change the hash generation.
    """
    df_blocks = calculate_intervals(df_reduced, 'pool', 'interval', SHIFT_PERIODS)
    df_blocks_full = calculate_other_intervals(df_blocks, 'pool')
    interval_dataframes = create_interval_dataframes(df_blocks_full, df_reduced, 'pool')
    return df_blocks, df_blocks_full, interval_dataframes

def save_interim_results(df_reduced, df_blocks, df_blocks_full, interval_dataframes, results_dir):
    os.makedirs(results_dir, exist_ok=True)
    df_reduced.to_csv(os.path.join(results_dir, "df_reduced.csv"), index=False)
    df_blocks.to_csv(os.path.join(results_dir, "df_blocks.csv"), index=False)
    df_blocks_full.to_csv(os.path.join(results_dir, "df_blocks_full.csv"), index=False)
    pickle_filepath = os.path.join(results_dir, "interval_dataframes.pickle")
    with open(pickle_filepath, "wb") as pickle_file:
        pickle.dump(interval_dataframes, pickle_file)

def log_count_transaction_types(df, log_comment=''):
    print('Total transactions - {}:'.format(log_comment))
    pairs = [tuple(x) for x in df[['pool', 'transaction_type']].values]
    counter = Counter(pairs)
    sorted_counter = sorted(counter.items(), key=lambda x: (x[0][0], x[0][1]))
    print(sorted_counter)


def main():
    ## DEX Data
    # Read uniswap cleansed data
    uniswap_filepath = "Data/cleansed/uniswap.csv"
    df_uniswap = clean_uniswap_data(uniswap_filepath)

    # Read etherscan cleansed data
    etherscan_filepath = "Data/cleansed/etherscan.csv"
    df_etherscan = clean_etherscan_data(etherscan_filepath)

    # Merge uniswap and etherscan dataframes
    df_dex = pd.merge(df_uniswap, df_etherscan, how='inner', left_on='id', right_on='hash')
    print('Data loss after merge with etherscan: ', len(df_uniswap) - len(df_dex))

    # Preprocess the data and reduce mints on the same block
    df_dex_preprocessed = preprocess_data(df_dex)
    df_dex_reduced = clean_mint_transactions(df_dex_preprocessed)

    # Logs for data loss
    log_count_transaction_types(df_uniswap)
    log_count_transaction_types(df_dex_reduced, 'after reducing mints')

    # Print table of reduced mint transactions
    df_reduced_mints = df_dex_reduced[df_dex_reduced['transaction_type'] == 'mints']
    df_uniswap_mints = df_uniswap[df_uniswap['transaction_type'] == 'mints']
    print('Mint loss in percentage: ', (len(df_uniswap_mints) - len(df_reduced_mints)) / len(df_uniswap_mints) * 100)

    # Infer block intervals and save results
    results_dir = "Data/interim_results"
    df_blocks, df_blocks_full, interval_dataframes = infer_block_intervals(df_dex_reduced)
    save_interim_results(df_dex_reduced, df_blocks, df_blocks_full, interval_dataframes, results_dir)

if __name__ == "__main__":
    main()



