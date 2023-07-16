import os
import pandas as pd
from utils.horizon_aggregates import organize_dex_data_on_horizons

interim_results_dir = "Data/interim_results"

# Load DEX transactions
df_reduced = pd.read_csv(os.path.join(interim_results_dir, "df_reduced.csv"))

# Load horizons
df_horizons = pd.read_csv(os.path.join(interim_results_dir, "df_horizons.csv"))

df_dex_horizons = organize_dex_data_on_horizons(df_reduced, df_horizons, step=10)







reference_pool_mint = 500
reference_block_label = 'reference_blockNumber_500'

# Define the directory path where the files are saved
interim_results_dir = "Data/interim_results"
processed_results_dir = "Data/processed"

# Open the CSV files as DataFrames
df_reduced = pd.read_csv(os.path.join(interim_results_dir, "df_reduced.csv"))
df_blocks_full = pd.read_csv(os.path.join(interim_results_dir, "df_blocks_full.csv"))
df_direct_pool = pd.read_csv(os.path.join(processed_results_dir, "direct_pool.csv"), index_col=0)


# Calculate the horizons for the reduced DataFrame
df_horizons = calculate_horizons(df_reduced, step=10)



mints_null_counts = df_direct_pool.isnull().sum()
mints_null_counts.sort_values(ascending=False, inplace=True)



# ## CEX Data
# # Read binance cleansed data
# binance_filepath = "Data/cleansed/binance.csv"
# df_binance = pd.read_csv(binance_filepath)
# df_binance['time'] = pd.to_datetime(df_binance['time'])

## Merge DEX and CEX data
# df_ex = pd.merge(df_direct_pool, df_binance, how='inner', left_on='timestamp', right_on='time')
# print('Data loss after merge with binance: ', len(df_direct_pool) - len(df_ex))





# Filter df_blocks_full to only include the reference pool
df_blocks = df_blocks_full[['hashid', 'blockNumber']].where(df_blocks_full['pool'] == reference_pool_mint)
# Assert unique blockNumber values, after removing null values
assert df_blocks[df_blocks['blockNumber'].notnull()]['blockNumber'].is_unique

# join df_direct_pool with df_blocks
df_blocks = df_blocks.rename(columns={'blockNumber': reference_block_label})
df_reference = df_direct_pool.merge(df_blocks, how='left', left_index=True, right_on='hashid')

# join df with df_reference on reference_blockNumber
df = df_horizons.merge(df_reference, how='left', on=reference_block_label)
null_counts = df.isnull().sum()



# Showcase tables
pass










showcase_cols = ['blockNumber', 'min_flag', 'reference_blockNumber', 'horizon_label', 'cum_volume_500']
df_horizons.iloc[108757:109052][showcase_cols]

#df.iloc[108757:109052][['blockNumber', 'reference_blockNumber_500', 'reference_blockNumber_3000', 'horizon', 'blsame_1', 'cum_volume_500']]
#15552772


# NOTE -> We have duplicated reference_blockNumber values. This is because we have multiple pools that have the same reference_blockNumber.
# But since we are filtering df_blocks_full to only include the reference pool, we should not have this duplication.
# Remove null values from analysis
df_notnull = df_reference[df_reference[reference_block_label].notnull()]
dups = df_notnull[df_notnull[reference_block_label].duplicated()][reference_block_label]
assert len(dups) == 0

# # For analysis on full blocks (in the future, and if needed):
# dup_example = df_notnull[df_notnull[reference_block_label].duplicated()].iloc[0][reference_block_label]
# df_reference[df_reference['reference_blockNumber'] == dup_example]
# df[df['reference_blockNumber'] == dup_example]


#We now proceed to a detailed investigation of our forecasting ability regarding the incoming trading volume on pool 3000, after a mint operation occurred on pool 500
target_variable = 'cum_volume_3000_ref500'
explainable_variables = df_direct_pool.columns.tolist() # For now, all columns. Later, we will do some aggregations, etc.

