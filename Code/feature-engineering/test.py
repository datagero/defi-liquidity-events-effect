#%%
import pandas as pd
import numpy as np
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
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s').dt.tz_localize('UTC')

#  'blockNumber' column is in hexadecimal form (beginning with '0x'). We need to convert it to an integer value to perform calculations on it
df['blockNumber'] = df['blockNumber'].apply(lambda x: int(x, 16))  # Convert hexadecimal to integer

df_sorted_timestamp = df.sort_values(by='timestamp')
df_sorted_blocknumber = df.sort_values(by='blockNumber')
block_number_order_same = (df_sorted_timestamp['blockNumber'].values == df_sorted_blocknumber['blockNumber'].values).all()
assert block_number_order_same, "Sorting by timestamp and block number does not result in the same block number order"


def calculate_intervals(df, pool_col, interval_col, l_values):
    dfs = []  # List to store the modified dataframes
    df = df[df['transaction_type'] == 'mints']
    for pool in df[pool_col].unique():
        df_pool = df[df[pool_col] == pool].copy()  # Create a copy of the slice
        for l in l_values:
            df_pool[f'{interval_col}_{l}'] = df_pool['blockNumber'].shift(l)
        
        # Create a new column for the block number chain
        block_numbers = df_pool[[f'{interval_col}_{l}' for l in l_values]].apply(lambda row: sorted(row), axis=1)
        df_pool['blockNumberChain'] = block_numbers
        
        dfs.append(df_pool)  # Append the modified dataframe to the list
    
    df_merged = pd.concat(dfs)  # Concatenate all dataframes
    return df_merged


def calculate_size_previous_mints(df, pool_col, size_col, l_values):
    for pool in df[pool_col].unique():
        df_pool = df[df[pool_col] == pool]
        for l in l_values:
            df[f'{size_col}_same_{l}'] = df_pool[size_col].shift(l)
        df[f'{size_col}_other_{l}'] = df.groupby(pool_col)[f'{size_col}_same_{l}'].shift(-1)
    return df

# def calculate_width_previous_mints(df, pool_col, width_col, l_values):
#     for pool in df[pool_col].unique():
#         df_pool = df[df[pool_col] == pool]
#         for l in l_values:
#             df[f'{width_col}_same_{l}'] = df_pool[width_col].shift(l)
#         df[f'{width_col}_other_{l}'] = df.groupby(pool_col)[f'{width_col}_same_{l}'].shift(-1)
#     return df



def calculate_volatility(df: pd.DataFrame, pool_col: str, price_col: str, l_values: list) -> pd.DataFrame:
    for l in l_values:
        df[f'volatility_{l}'] = np.nan
        for idx, row in df.iterrows():
            mint_pool = row[pool_col]
            mint_block = row['blockNumber']
            same_pool_mints = df[(df[pool_col] == mint_pool) & (df['blockNumber'] < mint_block)]
            expanding_intervals = same_pool_mints[-l:]
            if len(expanding_intervals) >= l:
                pool_prices = expanding_intervals[price_col]
                volatility = np.std(pool_prices)
                df.at[idx, f'volatility_{l}'] = volatility

    return df

def calculate_rate_of_traded_volume(df, pool_col, volume_col, shift_periods):
    pools = df[pool_col].unique()
    for pool in pools:
        df_pool = df[df[pool_col] == pool]
        for shift_period in shift_periods:
            # Calculate the start block number based on the shift period
            start_block = df_pool['blockNumber'].shift(shift_period)
            
            # Filter transactions within the block range and pool of interest
            df_interval = df_pool[(df_pool['blockNumber'] >= start_block) & (df_pool[pool_col] == pool)]
            
            # Calculate the traded volume in USD
            traded_volume = df_interval[volume_col].sum()
            
            # Calculate the duration of the interval
            duration = shift_period
            
            # Calculate the rate of traded volume in USD
            rate = traded_volume / duration
            
            # Assign the rate to the corresponding column
            df.loc[df_interval.index, f'rate-USD-{shift_period}-{pool}'] = rate
    
    return df


def calculate_rate_of_trade_count(df, pool_col, count_col, shift_periods):
    pools = df[pool_col].unique()
    for pool in pools:
        df_pool = df[df[pool_col] == pool]
        for shift_period in shift_periods:
            # Calculate the start block number based on the shift period
            start_block = df_pool['blockNumber'].shift(shift_period)
            
            # Filter transactions within the block range and pool of interest
            df_interval = df_pool[(df_pool['blockNumber'] >= start_block) & (df_pool[pool_col] == pool)]
            
            # Calculate the count of trades
            trade_count = df_interval[count_col].count()
            
            # Calculate the duration of the interval
            duration = shift_period
            
            # Calculate the rate of trade count
            rate = trade_count / duration
            
            # Assign the rate to the corresponding column
            df.loc[df_interval.index, f'rate-count-{shift_period}-{pool}'] = rate
    
    return df


def calculate_average_trade_size(df, pool_col, size_col, intervals):
    for interval in intervals:
        start_block, end_block = interval.split('e')
        start_block = int(start_block)
        end_block = int(end_block)
        
        # Filter transactions within the block range and pools of interest
        df_interval = df[(df['blockNumber'] >= start_block) & (df['blockNumber'] <= end_block) & (df[pool_col].str.contains('WBTC-WETH'))]
        
        # Calculate the average trade size in USD
        average_size = df_interval[size_col].mean()
        
        # Assign the average trade size to the corresponding column
        df.loc[df_interval.index, f'avg-USD-{interval}'] = average_size
    
    return df



# the pool price represents the ratio of WBTC tokens to WETH tokens in the pool.
df['pool_price'] = df['amount1'] / df['amount0']

shift_periods = range(1, 4)

df = calculate_intervals(df, 'pool', 'interval', shift_periods)
df = calculate_size_previous_mints(df, 'pool', 'amountUSD', shift_periods)
# df = calculate_width_previous_mints(df, 'pool', 'width', [1, 2, 3])
df = calculate_volatility(df, 'pool', 'pool_price', shift_periods)

df = calculate_rate_of_traded_volume(df, 'pool', 'amountUSD', shift_periods)
df = calculate_rate_of_trade_count(df, 'pool', 'transactionIndex', shift_periods)


# Print the updated DataFrame
print(df)





pass
