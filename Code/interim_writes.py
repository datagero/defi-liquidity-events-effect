import os
import pickle
import pandas as pd
import numpy as np
from collections import Counter
from feature_engineering.build_intervals import calculate_intervals, calculate_other_intervals, create_interval_dataframes, reduce_mints

SAME_POOL = 500
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

def merge_dataframes(uniswap_df, etherscan_df):
    """
    TODO - For now, do inner join. Once we have full 6 month data, plan is to change to left / do missing value analysis.
    """
    df = pd.merge(uniswap_df, etherscan_df, how='inner', left_on='id', right_on='hash')
    return df

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

    print('Total uniswap<>etherscan transactions:')
    pairs = [tuple(x) for x in df_reduced[['pool', 'transaction_type']].values]
    pairs.sort()
    counter = Counter(pairs)
    sorted_counter = sorted(counter.items(), key=lambda x: (x[0][0], x[0][1]))
    print(sorted_counter)
    return df_reduced

def infer_block_intervals(df_reduced):
    """
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

def main():
    # Read uniswap cleansed data
    uniswap_filepath = "Data/cleansed/uniswap.csv"
    df_uniswap = clean_uniswap_data(uniswap_filepath)

    print('Total uniswap transactions:')
    pairs = [tuple(x) for x in df_uniswap[['pool', 'transaction_type']].values]
    counter = Counter(pairs)
    sorted_counter = sorted(counter.items(), key=lambda x: (x[0][0], x[0][1]))
    print(sorted_counter)

    # Read etherscan cleansed data
    etherscan_filepath = "Data/cleansed/etherscan.csv"
    df_etherscan = clean_etherscan_data(etherscan_filepath)

    # Merge uniswap and etherscan dataframes
    df = merge_dataframes(df_uniswap, df_etherscan)

    # Preprocess the data
    df_preprocessed = preprocess_data(df)

    # Analyze mints
    df_reduced = clean_mint_transactions(df_preprocessed)

    # Infer block intervals and save results
    results_dir = "Data/interim_results"
    df_blocks, df_blocks_full, interval_dataframes = infer_block_intervals(df_reduced)
    save_interim_results(df_reduced, df_blocks, df_blocks_full, interval_dataframes, results_dir)

if __name__ == "__main__":
    main()



