import pandas as pd
from feature_engineering.build_intervals import *
from feature_engineering.calculations import *
import random
import math

#TODO - Good progress! 
# Now need to calculate the same and other pool values, and create a dataframe with all this data
# Need to get the distance function
# Then, through data cleaning need to generate TVL, width and size

# Sample data
factor = 1
data = {
    'transaction_type': ['mints', 'burns', 'swaps', 'mints', 'swaps', 'mints', 'burns', 'mints', 'burns', 'swaps', 'mints', 'swaps', 'mints', 'burns', 'mints', 'burns', 'swaps', 'mints', 'swaps', 'mints', 'mints', 'mints', 'mints'] * factor,
    'pool': [2, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 2, 2, 2, 1, 1, 2, 2, 2, 1, 2, 2, 1] * factor,
    'blockNumber': [100, 105, 107, 113, 115, 120, 123, 124, 133, 136, 163, 178, 180, 185, 187, 187, 194, 195, 195, 207, 230, 247, 249],
    'size': [1.5, 1.2, 0.8, 2.0, 1.0, 1.8, 1.5, 2.2, 1.9, 1.7, 1.4, 1.6, 1.3, 1.1, 1.2, 1.8, 1.5, 1.7, 1.6, 1.4, 1.5, 1.6, 1.7] * factor,
    'width': [100, 95, 115, 110, 105, 102, 100, 120, 125, 115, 110, 108, 100, 105, 102, 100, 98, 96, 95, 100, 100, 102, 105] * factor,
    'pool_price': [100, 98, 110, 105, 102, 102, 100, 150, 152, 148, 145, 147, 150, 148, 145, 143, 142, 140, 138, 140, 141, 142, 143] * factor,
    'amountUSD': [1500, 1200, 800, 2000, 1000, 1800, 1500, 2200, 1900, 1700, 1400, 1600, 1300, 1100, 1200, 1800, 1500, 1700, 1600, 1400, 1500, 1600, 1700] * factor
}


df = pd.DataFrame(data)
df['timestamp'] = pd.to_datetime(df['blockNumber'], unit='s')

shift_periods = range(0, 4)
df_blocks = calculate_intervals(df, 'pool', 'interval', shift_periods)
df_blocks_full = calculate_other_intervals(df_blocks, pool_same=1)


pass

# TODO - Do this for each pool separately
# Create dataframes for each interval
interval_dataframes = create_interval_dataframes(df_blocks_full, df, pool_same=1)

data = {}
for hash, intervals_dict in interval_dataframes['same'].items():

    if hash not in data:
        data[hash] = {}

    hash_df = pd.DataFrame()

    max_interval = max([int(x) for x in intervals_dict.keys()])
    for interval, interval_dict in intervals_dict.items():
        lbl = int(interval)
        lbl_to_next = f"{lbl}{lbl+1}"
        lbl_from_root = f"0_{lbl+1}"

        df_interval = interval_dict['df']
        block_time = interval_dict['blockTime']
        hash_df = pd.concat([hash_df, df_interval])

        sl = get_size(df_interval[df_interval['transaction_type']=='mints'], 'size')
        wl = get_width(df_interval[df_interval['transaction_type']=='mints'], 'width')
        rateUSD = calculate_traded_volume_rate(df_interval, 'amountUSD')
        rateCount = calculate_trades_count(df_interval)
        avgUSD = calculate_average_volume(df_interval, 'amountUSD')

        # Only same pool
        vol = calculate_volatility(hash_df, 'pool_price')

        if lbl == 0:
            data[hash].update({
                f's0': block_time,
                f'w0': sl
                }) 
        else:
            data[hash].update({
                f'blsame_{lbl}': block_time,
                f'slsame_{lbl}': sl,
                f'wlsame_{lbl}': wl,
                })

        # If its the last label, skip
        if lbl != max_interval:
            data[hash].update({
                f'rate-USD-isame_{lbl_to_next}': rateUSD,
                f'rate-count-isame_{lbl_to_next}': rateCount,
                f'avg-USD-isame_{lbl_to_next}': avgUSD,
                f'vol_{lbl_from_root}': vol,
                })

for hash, intervals in interval_dataframes['other'].items():
    if hash not in data:
        data[hash] = {}

    max_interval = max([int(x) for x in intervals_dict.keys()])
    for interval, interval_dict in intervals_dict.items():
        lbl = int(interval)
        lbl_to_next = f"{lbl}{lbl+1}"
        lbl_from_root = f"0_{lbl+1}"

        df_interval = interval_dict['df']
        block_time = interval_dict['blockTime']
        
        sl = get_size(df_interval[df_interval['transaction_type']=='mints'], 'size')
        wl = get_width(df_interval[df_interval['transaction_type']=='mints'], 'width')
        rateUSD = calculate_traded_volume_rate(df_interval, 'amountUSD')
        rateCount = calculate_trades_count(df_interval)
        avgUSD = calculate_average_volume(df_interval, 'amountUSD')

        if lbl == 0:
            data[hash].update({
                f's0': block_time,
                f'w0': sl
                }) 
        else:
            data[hash].update({
                f'blother_{lbl}': block_time,
                f'slother_{lbl}': sl,
                f'wlother_{lbl}': wl,
                })

        # If its the last label, skip
        if lbl != max_interval:
            data[hash].update({
                f'rateUSD-iother_{lbl_to_next}': rateUSD,
                f'ratecount-iother_{lbl_to_next}': rateCount,
                f'avg-USD-iother_{lbl_to_next}': avgUSD,
                # f'vol_{lbl_from_root}': vol,
                })



# Get the max value length number from the nested data dictionary
max_value_length = max([len(v) for v in data.values()])
# Get the key that corresponds to the max value length number
max_value_length_key = [k for k, v in data.items() if len(v) == max_value_length][0]
data[max_value_length_key]


test = interval_dataframes[3223564584]

len(data[924938349])

# 3*3*2 = 18 features (same + other pool)
#TODO- Distance
get_size(test[test['transaction_type']=='mints'], 'size')
get_width(test[test['transaction_type']=='mints'], 'width')

# 3*2 = 6 features (only same pool)
calculate_volatility(test['2_3'], 'pool_price')

# 3*3*2 = 18 features (same + other pool)
calculate_traded_volume_rate(test, 'amountUSD')
calculate_trades_count(test)
calculate_average_volume(test, 'amountUSD')

# 1 feature
# Need to check how to get the Total Value Locked (TVL)
# To approximate the Total Value Locked (TVL) in the pools,
# you can compute the cumulative sum in USD of the mint and burn operations for both pools from May 2021,
# which is when Uniswap v3 was deployed. By summing up the USD values of the mint and burn operations,
# you can estimate the TVL in the pools over a specific period.
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
