#%%
"""
Notes -> Have just developed the block number chain feature.
Need to adapt all other functions to work at block level (as per mints)
"""
from tqdm import tqdm
import pandas as pd
import numpy as np
import ast
import zlib
import copy

def generate_hash(df):
    # Convert timestamp column to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Sort the DataFrame by timestamp in ascending order
    df_sorted = df.sort_values(by='timestamp')

    # Generate hashids based on the sorted index
    df_sorted['hashid'] = df_sorted.apply(lambda row: zlib.crc32((str(row['pool']) + str(row['blockNumber'])).encode()), axis=1)

    # Validate uniqueness of reference blockNumber and pool
    assert df_sorted[['blockNumber', 'pool']].duplicated().any() == False, "Reference blockNumber and pool are not unique"
    assert df_sorted['hashid'].duplicated().any() == False, "hashid is not unique"

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
        block_numbers = df_pool[[f'{interval_col}_{l}' for l in l_values]].apply(lambda row: sorted(row, reverse=True), axis=1)
        df_pool['blockNumberChain'] = block_numbers

        dfs.append(df_pool)  # Append the modified dataframe to the list

    df_merged = pd.concat(dfs)  # Concatenate all dataframes
    df_hashed = generate_hash(df_merged)
    
    return df_hashed[['hashid', 'pool', 'timestamp', 'blockNumber', 'blockNumberChain']]

def calculate_other_intervals(df, pool_col):
    """
    Calculate intervals and generate a hashed dataframe.

    Args:
        df (DataFrame): Input DataFrame.
        pool_col (str): Name of the column representing the pool.
    Returns:
        DataFrame: Dataframe containing the calculated intervals for 'other' pool.

    """
    # Initialise empty dataframe
    result_df = pd.DataFrame()

    # Get unique pool values
    unique_pools = df[pool_col].unique()
    for pool_same in unique_pools:

        df_same = df[df['pool'] == pool_same].copy().reset_index(drop=True).copy(deep=True)
        df_other = df[df['pool'] != pool_same].copy().reset_index(drop=True).copy(deep=True)

        # Calculate intervals for other pool
        # Get length of blockNumberChain and set default other_blockNumberChain
        chain_length = len(df['blockNumberChain'].iloc[0])
        df_same['other_blockNumberChain'] = [[np.nan]*chain_length]*df_same.shape[0]
        for i in range(len(df_same)):
            reference_block = df_same.loc[i, 'blockNumber']
            max_block_pool2 = df_other.loc[(df_other['blockNumber'] < reference_block), 'blockNumber'].max()

            other_chain_raw = df_other.loc[df_other['blockNumber'] == max_block_pool2, 'blockNumberChain'].values
            if other_chain_raw.shape[0] == 0:
                # leave default
                other_chain = df_same.loc[i, 'other_blockNumberChain'][:]
            else:
                other_chain = other_chain_raw[0][:]

            # insert reference block at the start of the chain
            if np.isnan(other_chain[0]) or int(other_chain[0]) != reference_block:
                other_chain.insert(0, reference_block)
                other_chain = other_chain[:-1][:]

            df_same.loc[i, 'other_blockNumberChain'] = str(other_chain)

        # Append to result_df
        result_df = pd.concat([result_df, df_same])

    return result_df

def create_interval_dataframes(df_blocks, df, pool_col):
    """
    Create interval-based dataframes based on specified columns.

    Args:
        df_blocks (DataFrame): DataFrame containing block information.
        df (DataFrame): DataFrame containing the main data.
        end_col (str): Name of the column representing the end of the interval.
        pool_col (str): Name of the column representing the pool.

    Returns:
        dict: A nested dictionary with interval-based dataframes, where the keys are the hash IDs.

    """

    def update_interval(chain_col, df_blocks, df):
        
        dataframes = {}
        for _, row in df_blocks.iterrows():
            ## For debugging specific hashes
            if row['hashid'] == 576279937:
                pass
            chain = row[chain_col]
            if isinstance(chain, str): 
                chain = chain.replace('nan', 'None')
                chain = ast.literal_eval(chain)
                chain = [np.nan if val is None else val for val in chain]
            max_index = max((i for i, x in enumerate(chain) if not np.isnan(x)), default=None)

            reference_start = int(chain[0])
            for i in range(max_index):
                interval_start = int(chain[i])
                interval_end = int(chain[i+1])
                if pd.notna(interval_start) and pd.notna(interval_end):
                    df_interval = df[(df['blockNumber'] <= interval_start) & (df['blockNumber'] > interval_end)].copy()
                    if row['hashid'] not in dataframes:
                        dataframes[row['hashid']] = {}

                    #key = f"{i}_{i+1}"
                    key = f"{i}"
                    dataframes[row['hashid']][key] = {
                        'blockTime': reference_start - interval_end,
                        'df': df_interval}
            if max_index > 0:
                # Get last mint operation
                interval_end = int(chain[i+1])
                if pd.notna(interval_start) and pd.notna(interval_end):
                    df_interval = df[df['blockNumber'] == interval_end].copy()

                    key = i+1
                    dataframes[row['hashid']][key] = {
                        'blockTime': reference_start - interval_end,
                        'df': df_interval}
        
        return dataframes

    # Initialise empty dictionary store
    interval_dataframes = {}

    # Get unique pool values
    unique_pools = df[pool_col].unique()

    for pool_same in unique_pools:

        pools = {}

        # Get same and other pool dataframes (to get intervals)
        df_same = df[df['pool'] == pool_same].copy(deep=True).reset_index(drop=True)
        df_other = df[df['pool'] != pool_same].copy(deep=True).reset_index(drop=True)

        # Get same pool blocks (these should already have  blockNumberChain and other_blockNumberChain)
        df_blocks_same = df_blocks[df_blocks['pool'] == pool_same].copy(deep=True).reset_index(drop=True)
        
        pools['same'] = update_interval('blockNumberChain', df_blocks_same, df_same)
        pools['other'] = update_interval('other_blockNumberChain', df_blocks_same, df_other)

        interval_dataframes[pool_same] = pools

    return interval_dataframes

def reduce_mints(df):
    """
    According to the main paper:
    Group LP mint events that happen on the same block. Instances of the latter are rare (i.e. ∼ 2% of our entries) and are aggregated for later
    convenience into a single operation of value equal to the sum, and width equal of the average of the related entries.
    """

    # Create a copy of the input DataFrame
    df = df.copy()

    df_nonmints = df[df['transaction_type'] != 'mints']
    df_mints = df[df['transaction_type'] == 'mints']

    # Group the DataFrame by the pool and block number
    grouped = df_mints.groupby(['timestamp', 'transaction_type', 'pool', 'blockNumber'])

    # Calculate the aggregated values for each group
    aggregated = grouped.agg({'size': 'sum', 'width': 'mean', 'pool_price': 'mean', 'amountUSD': 'sum'})

    # Append the aggregated rows to df_nonmints
    df_reduced = pd.concat([df_nonmints, aggregated.reset_index()], ignore_index=True)

    # Sort the DataFrame by pool and blockNumber
    df_reduced = df_reduced.sort_values(by=['pool', 'blockNumber'])

    return df_reduced

def calculate_horizons(df, step):
    # For now only mints on pool 3000
    df_mints = df[(df['transaction_type']=='mints')]
    # df_mints = df[df['transaction_type']=='mints']

    block_numbers = df_mints['blockNumber'].sort_values().values
    expanded_blocks = []

    # for each pair of consecutive blocks
    for start, end in zip(block_numbers[:-1], block_numbers[1:]):
        # generate the sequence of blocks and add to the list
        expanded_blocks.append(np.arange(start, end, step))

    # flattening the list of arrays and making a Series
    expanded_series = pd.Series(np.concatenate(expanded_blocks))

    # adding the last block from the original series
    expanded_series = pd.concat([expanded_series, pd.Series(block_numbers[-1])])

    # creating a DataFrame
    df_expanded = pd.DataFrame(expanded_series, columns=['blockNumber'])

    # Calculate the difference between consecutive block numbers and fill missing values with 0
    # Set min_flag to 1 if the blockNumber is present in df_mints, otherwise set it to 0
    # Set the reference_blockNumber to the blockNumber when min_flag is 1, and forward fill missing values
    # Group the DataFrame by reference_blockNumber and calculate the cumulative count within each group, starting from 1
    df_expanded['horizon'] = df_expanded['blockNumber'].diff().fillna(0)
    df_expanded['min_flag'] = df_expanded['blockNumber'].isin(df_mints['blockNumber']).astype(int)
    df_expanded['min_flag_500'] = df_expanded['blockNumber'].isin(df_mints[df_mints['pool'] == 500]['blockNumber']).astype(int)
    df_expanded['min_flag_3000'] = df_expanded['blockNumber'].isin(df_mints[df_mints['pool'] == 3000]['blockNumber']).astype(int)
    
    df_expanded['reference_blockNumber'] = pd.Series(np.where(df_expanded['min_flag'] == 1, df_expanded['blockNumber'], np.nan)).ffill().astype(int)
    df_expanded['reference_blockNumber_500'] = pd.Series(np.where(df_expanded['min_flag_500'] == 1, df_expanded['blockNumber'], np.nan)).ffill().astype(int)
    df_expanded['reference_blockNumber_3000'] = pd.Series(np.where(df_expanded['min_flag_3000'] == 1, df_expanded['blockNumber'], np.nan)).ffill().fillna(-1).astype(int)
    df_expanded['horizon_label'] = df_expanded.groupby('reference_blockNumber').cumcount() + 1
    df_expanded['horizon_label_500'] = df_expanded.groupby('reference_blockNumber').cumcount() + 1
    df_expanded['horizon_label_3000'] = df_expanded.groupby('reference_blockNumber').cumcount() + 1
    df_expanded = df_expanded.reset_index(drop=True)

    # Calculate cummulative incoming volume
    # For now, assume only for swaps.
    df_swaps = df[df['transaction_type']=='swaps']
    df_grouped = df_swaps.groupby(['blockNumber', 'pool'])['amountUSD'].sum().reset_index()
    df_pivoted = df_grouped.pivot(index='blockNumber', columns=['pool'], values='amountUSD')
    df_pivoted.columns = ['volume_500', 'volume_3000']


    df_pivoted = df_pivoted.reset_index()

    # Get closest blockNumber from the base table
    df_reference_sorted = df_expanded.sort_values('blockNumber')
    indices = df_reference_sorted['blockNumber'].searchsorted(df_pivoted['blockNumber'], side='right') - 1
    clipped_indices = np.clip(indices, 0, len(df_reference_sorted) - 1)
    df_pivoted['closest_blockNumber'] = df_reference_sorted.loc[clipped_indices, 'blockNumber'].values
    df_pivoted['closest_blockNumber'] = df_pivoted['closest_blockNumber'].astype(int)

    # Aggregate sum incoming volume into the closest reference_blockNumber
    df_volumes = df_pivoted.groupby('closest_blockNumber').sum().reset_index().drop(columns=['blockNumber'])

    # Merge with base expanded dataframe for a complete view
    df_merged = df_expanded.merge(df_volumes, left_on='blockNumber', right_on='closest_blockNumber', how='left')

    df_merged[['volume_500', 'volume_3000']] = df_merged[['volume_500', 'volume_3000']].fillna(0)

    # Create cumulative volume columns for each reference_blockNumber
    df_merged['cum_volume_500'] = df_merged.groupby('reference_blockNumber')['volume_500'].cumsum()
    df_merged['cum_volume_3000'] = df_merged.groupby('reference_blockNumber')['volume_3000'].cumsum()

    df_merged['cum_volume_500_ref500'] = df_merged.groupby('reference_blockNumber_500')['volume_500'].cumsum()
    df_merged['cum_volume_3000_ref500'] = df_merged.groupby('reference_blockNumber_500')['volume_3000'].cumsum()

    df_merged['cum_volume_500_ref3000'] = df_merged.groupby('reference_blockNumber_3000')['volume_500'].cumsum()
    df_merged['cum_volume_3000_ref3000'] = df_merged.groupby('reference_blockNumber_3000')['volume_3000'].cumsum()

    return df_merged


#15552772-15555597#15555594 

# df_pivoted[(df_pivoted.index>=15552764) & (df_pivoted.index <= 15555597)]


# df = df_merged
# df[(df['blockNumber']>=15552764) & (df['blockNumber'] <= 15555597)]