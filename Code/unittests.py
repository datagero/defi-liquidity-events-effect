import pandas as pd

# Sample data
data = {
    'transaction_type': ['mint', 'burn', 'swap', 'mint', 'swap', 'mint', 'burn'],
    'pool': [1, 1, 2, 1, 2, 1, 2],
    'blockNumber': [100, 120, 140, 150, 170, 180, 200],
    'size': [1.5, 1.2, 0.8, 2.0, 1.0, 1.8, 1.5],
    'width': [100, 95, 115, 110, 105, 102, 100],
    'price': [100, 98, 110, 105, 102, 102, 100]
}

df = pd.DataFrame(data)

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
