#%%
"""
Notes -> Have just developed the block number chain feature.
Need to adapt all other functions to work at block level (as per mints)
"""

import pandas as pd
import numpy as np

import zlib
# Read uniswap cleansed data
df = pd.read_csv("Data/cleansed/uniswap.csv")

# Clean Id - Remove the trailing '#' and everything after it
df['id'] = df['id'].str.split('#').str[0]

# Read etherscan cleansed data
etherscan = pd.read_csv("Data/cleansed/etherscan.csv")

# join uniswap and etherscan data on transactionIndex and id
df = pd.merge(df, etherscan, how='left', left_on='id', right_on='hash')

#%%
# Ensure data is sorted by timestamp -> Move this to cleaning
df = df.sort_values(by='timestamp')
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s').dt.tz_localize('UTC')

#  'blockNumber' column is in hexadecimal form (beginning with '0x'). We need to convert it to an integer value to perform calculations on it
df['blockNumber'] = df['blockNumber'].apply(lambda x: int(x, 16))  # Convert hexadecimal to integer

# Move this to data validation
df_sorted_timestamp = df.sort_values(by='timestamp')
df_sorted_blocknumber = df.sort_values(by='blockNumber')
block_number_order_same = (df_sorted_timestamp['blockNumber'].values == df_sorted_blocknumber['blockNumber'].values).all()
assert block_number_order_same, "Sorting by timestamp and block number does not result in the same block number order"


def generate_hash(df):
    # Convert timestamp column to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Sort the DataFrame by timestamp in ascending order
    df_sorted = df.sort_values(by='timestamp')

    # Generate hashids based on the sorted index
    df_sorted['hashid'] = df_sorted['blockNumberChain'].map(lambda x: zlib.crc32(str(x).encode()))

    return df_sorted

def calculate_intervals(df, pool_col, interval_col, l_values):
    """
    Calculate intervals and generate a hashed dataframe.

    Args:
        df (DataFrame): Input DataFrame.
        pool_col (str): Name of the column representing the pool.
        interval_col (str): Name of the column to calculate intervals from.
        l_values (list): List of values for calculating intervals.

    Returns:
        DataFrame: Hashed dataframe containing the calculated intervals.

    """
    dfs = []  # List to store the modified dataframes
    df = df.copy()
    df = df[df['transaction_type'] == 'mints']
    df = df[['pool', 'blockNumber', 'timestamp']]
    for pool in df[pool_col].unique():
        df_pool = df[df[pool_col] == pool].copy()  # Create a copy of the slice
        for l in l_values:
            df_pool[f'{interval_col}_{l}'] = df_pool['blockNumber'].shift(l)

        # Create a new column for the block number chain
        block_numbers = df_pool[[f'{interval_col}_{l}' for l in l_values]].apply(lambda row: sorted(row), axis=1)
        df_pool['blockNumberChain'] = block_numbers

        dfs.append(df_pool)  # Append the modified dataframe to the list

    df_merged = pd.concat(dfs)  # Concatenate all dataframes
    df_hashed = generate_hash(df_merged)
    
    return df_hashed

def create_interval_dataframes(df_blocks, df, end_col, start_col, column_mapping, dataframes):
    """
    Create interval-based dataframes based on specified columns.

    Args:
        df_blocks (DataFrame): DataFrame containing block information.
        df (DataFrame): DataFrame containing the main data.
        end_col (str): Name of the column representing the end of the interval.
        start_col (str): Name of the column representing the start of the interval.
        column_mapping (dict): Mapping of column names to their corresponding values.

    Returns:
        dict: A nested dictionary with interval-based dataframes, where the keys are the hash IDs.

    """
    for _, row in df_blocks.iterrows():
        interval_start = row[start_col]
        interval_end = row[end_col]
        if pd.notna(interval_start) and pd.notna(interval_end):
            df_interval = df[(df['blockNumber'] > interval_start) & (df['blockNumber'] <= interval_end)].copy()
            if row['hashid'] not in dataframes:
                dataframes[row['hashid']] = {}
            else:
                pass
            key = f"{column_mapping[end_col]}_{column_mapping[start_col]}"
            dataframes[row['hashid']][key] = df_interval
    return dataframes
