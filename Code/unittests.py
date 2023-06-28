import pandas as pd
from feature_engineering.test import *
from feature_engineering.calculations import *

#TODO - Good progress! 
# Now need to calculate the same and other pool values, and create a dataframe with all this data
# Need to get the distance function
# Then, through data cleaning need to generate TVL, width and size

# Sample data
factor = 1
data = {
    'transaction_type': ['mints', 'burns', 'swaps', 'mints', 'swaps', 'mints', 'burns', 'mints', 'burns', 'swaps', 'mints', 'swaps', 'mints', 'burns', 'mints', 'burns', 'swaps', 'mints', 'swaps', 'mints', 'mints', 'mints', 'mints'] * factor,
    'pool': [1, 1, 2, 1, 2, 1, 2, 1, 1, 2, 1, 2, 1, 2, 1, 1, 2, 1, 2, 2, 2, 2, 2] * factor,
    'blockNumber': [100, 120, 140, 150, 170, 180, 200, 220, 240, 260, 280, 300, 320, 340, 360, 380, 400, 420, 440, 460, 480, 500, 520] * factor,
    'size': [1.5, 1.2, 0.8, 2.0, 1.0, 1.8, 1.5, 2.2, 1.9, 1.7, 1.4, 1.6, 1.3, 1.1, 1.2, 1.8, 1.5, 1.7, 1.6, 1.4, 1.5, 1.6, 1.7] * factor,
    'width': [100, 95, 115, 110, 105, 102, 100, 120, 125, 115, 110, 108, 100, 105, 102, 100, 98, 96, 95, 100, 100, 102, 105] * factor,
    'pool_price': [100, 98, 110, 105, 102, 102, 100, 150, 152, 148, 145, 147, 150, 148, 145, 143, 142, 140, 138, 140, 141, 142, 143] * factor,
    'amountUSD': [1500, 1200, 800, 2000, 1000, 1800, 1500, 2200, 1900, 1700, 1400, 1600, 1300, 1100, 1200, 1800, 1500, 1700, 1600, 1400, 1500, 1600, 1700] * factor
}

df = pd.DataFrame(data)
df['timestamp'] = pd.to_datetime(df['blockNumber'], unit='s')

shift_periods = range(1, 4)
df_blocks = calculate_intervals(df, 'pool', 'interval', shift_periods)

pass

# TODO - Do this for each pool separately
# Call the function to create separate dataframes for interval_2 to interval_3
interval_dataframes = {}
interval_columns = ['blockNumber', 'interval_1', 'interval_2', 'interval_3']
column_mapping = {column: index for index, column in enumerate(interval_columns)}

# Iterate over the interval columns
for i in range(len(interval_columns) - 1):
    end_col = interval_columns[i]
    start_col = interval_columns[i + 1]
    
    # Create dataframes for each interval
    interval_dataframes.update(create_interval_dataframes(df_blocks, df, end_col, start_col, column_mapping))


test = interval_dataframes[244787741]['0_1']
# We got all 43 features

# 3*3*2 = 18 features (same + other pool)
#TODO- Distance
get_size(test[test['transaction_type']=='mints'], 'size')
get_width(test[test['transaction_type']=='mints'], 'width')

# 3*2 = 6 features (only same pool)
calculate_volatility(test, 'pool_price')

# 3*3*2 = 18 features (same + other pool)
calculate_traded_volume_rate(test, 'amountUSD')
calculate_trades_count(test)
calculate_average_volume(test, 'amountUSD')

# 1 feature
# Need to check how to get the Total Value Locked (TVL)
# To approximate the Total Value Locked (TVL) in the pools, you can compute the cumulative sum in USD of the mint and burn operations for both pools from May 2021, which is when Uniswap v3 was deployed. By summing up the USD values of the mint and burn operations, you can estimate the TVL in the pools over a specific period.
# calculate_tvl_difference_ratio(test, pool_col, tvl_col)

pass



#TODO -> Needs extension as i.e. lengths of the 01, e 02, e 03
#TODO -> There will be some interaction between pools in terms of time. i.e., the blocks are sequential, so we can track on same the last 3 triggers in other


# Print the separate dataframes
for i, df_interval in enumerate(interval_dataframes):
    print(f"Dataframe {i+1}:")
    print(df_interval)
    print()





# Legacy code
#====================================================================================================
# Calculate time in blocks from the previous three mint operations on the same pool
df_same_pool = df[df['transaction_type'] == 'mint']
df_same_pool['time_blocks_same_01'] = df_same_pool['blockNumber'] - df_same_pool['blockNumber'].shift(1)
df_same_pool['time_blocks_same_02'] = df_same_pool['blockNumber'] - df_same_pool['blockNumber'].shift(2)
df_same_pool['time_blocks_same_03'] = df_same_pool['blockNumber'] - df_same_pool['blockNumber'].shift(3)

# Calculate time in blocks from the previous three mint operations on the other pool
df_other_pool = df[df['transaction_type'] == 'mint']
df_other_pool['time_blocks_other_01'] = df_other_pool['blockNumber'] - df_other_pool['blockNumber'].shift(1)
df_other_pool['time_blocks_other_02'] = df_other_pool['blockNumber'] - df_other_pool['blockNumber'].shift(2)
df_other_pool['time_blocks_other_03'] = df_other_pool['blockNumber'] - df_other_pool['blockNumber'].shift(3)

# Calculate size and width of the current mint operation and previous three mint operations on both pools
df['size_current'] = df['size']
df['width_current'] = df['width']

df['size_previous_01'] = df[df['transaction_type'] == 'mint']['size'].shift(1)
df['width_previous_01'] = df[df['transaction_type'] == 'mint']['width'].shift(1)
df['size_previous_02'] = df[df['transaction_type'] == 'mint']['size'].shift(2)
df['width_previous_02'] = df[df['transaction_type'] == 'mint']['width'].shift(2)
df['size_previous_03'] = df[df['transaction_type'] == 'mint']['size'].shift(3)
df['width_previous_03'] = df[df['transaction_type'] == 'mint']['width'].shift(3)

# Calculate volatility of the pool price on the same pool over the 01, 02, 03 intervals
df_same_pool['volatility_same_01'] = df_same_pool[df_same_pool['pool'] == 1]['price'].rolling(2).std()
df_same_pool['volatility_same_02'] = df_same_pool[df_same_pool['pool'] == 1]['price'].rolling(3).std()
df_same_pool['volatility_same_03'] = df_same_pool[df_same_pool['pool'] == 1]['price'].rolling(4).std()

# Merge the calculated columns back to the original DataFrame
df = pd.concat([df, df_same_pool, df_other_pool], axis=1)

# Print the updated DataFrame
print(df)
