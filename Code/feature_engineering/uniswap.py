#%%
import pandas as pd
# Read uniswap cleansed data
df = pd.read_csv("Data/cleansed/uniswap.csv")

# Clean Id - Remove the trailing '#' and everything after it
df['id'] = df['id'].str.split('#').str[0]

# Read etherscan cleansed data
etherscan = pd.read_csv("Data/cleansed/etherscan.csv")

# join uniswap and etherscan data on transactionIndex and id
df = pd.merge(df, etherscan, how='left', left_on='id', right_on='hash')

#%%
# Ensure data is sorted by timestamp
df = df.sort_values(by='timestamp')

#  'blockNumber' column is in hexadecimal form (beginning with '0x'). We need to convert it to an integer value to perform calculations on it
df['blockNumber'] = df['blockNumber'].apply(lambda x: int(x, 16))  # Convert hexadecimal to integer

# Calculate the time difference between mint operations
df['time_diff'] = df[df['transaction_type'] == 'mints']['timestamp'].diff()

# Calculate the size of the mint operation
df['size'] = df['amountUSD']

# Calculate the width of the mint operation



# Past op


def calculate_previous_blocks(df):
    #Note: You need to ensure your dataframe is sorted by block number in ascending order before applying the above function.
    # Calculate block time difference for the last three mint operations
    df['prev_block1'] = df['blockNumber'].shift(1)   # Get previous block
    df['prev_block2'] = df['blockNumber'].shift(2)   # Get block before that
    df['prev_block3'] = df['blockNumber'].shift(3)   # Get block before that

    # Calculate difference in block number from the previous three blocks
    df['diff_block1'] = df['blockNumber'] - df['prev_block1']
    df['diff_block2'] = df['blockNumber'] - df['prev_block2']
    df['diff_block3'] = df['blockNumber'] - df['prev_block3']

    return df

df = calculate_previous_blocks(df)


def compute_past_operations(df, transaction_type='mints'):
    """
    Compute the past three operations for the given transaction type.

    Parameters:
    df (pd.DataFrame): DataFrame containing the transaction data.
    transaction_type (str): The transaction type to consider.

    Returns:
    pd.DataFrame: DataFrame with additional columns for past three operations.
    """
    # Filter out transactions of the given type and sort by timestamp
    trans_df = df[df['transaction_type'] == transaction_type].sort_values(by='timestamp')

    # Create a copy of the DataFrame to store shifted features
    shifted_df = trans_df.copy()

    # Shift the 'amount0', 'amount1' and 'timestamp' columns for the previous three operations
    for i in range(1, 4):
        shifted_df[f'past_amount0_{i}'] = trans_df['amount0'].shift(i)
        shifted_df[f'past_amount1_{i}'] = trans_df['amount1'].shift(i)
        shifted_df[f'past_timestamp_{i}'] = trans_df['timestamp'].shift(i)

    # Merge the original and shifted DataFrames
    final_df = pd.merge(trans_df, shifted_df, how='left', on='id')

    return final_df


final_df = compute_past_operations(df, 'mints')




def calculate_block_distance(df, pool_reference, shift_periods):
    for i in shift_periods:
        # For the same pool
        df[f'blsame_{i}'] = df[df['transaction_type'] == 'mints'][df['pool'] == pool_reference]['blockNumber'].shift(i) - df['blockNumber']
        # For the other pool
        df[f'blother_{i}'] = df[df['transaction_type'] == 'mints'][df['pool'] != pool_reference]['blockNumber'].shift(i) - df['blockNumber']
    return df

shift_periods = range(1, 4)  # For l ∈ {1, 2, 3}
df = calculate_block_distance(df, 3000, shift_periods)
