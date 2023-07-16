import os
import pandas as pd
from utils.build_intervals import calculate_horizons, calculate_horizons_v02
from utils.horizon_aggregates import organize_dex_data_on_horizons, organize_dex_data_on_horizons_v02, organize_cex_data_on_horizons

def validator_horizons(df1, dict2):
    """
    Validates that the horizons in df1 and dict2 are the same for each pool.

    Parameters:
    df1 (pd.DataFrame): First dataframe to compare.
    dict2 (dict): Dictionary of dataframes to compare.

    Returns:
    None. Raises exception if any of the comparisons fail.
    """
    def prepare_df_for_comparison(df, pool_flag, standard_cols):
        if pool_flag == 'base':
            return df[standard_cols]
        else:
            cols = [f"{x}_{pool_flag}" for x in ['min_flag', 'reference_blockNumber', 'horizon_label']]
            cols.insert(0, 'blockNumber')
            cols.insert(1, 'horizon')

            df_subset = df[cols]
            df_subset.columns = standard_cols
            return df_subset.drop(columns=['horizon', 'horizon_label'])

    standard_cols = ['blockNumber', 'horizon', 'min_flag', 'reference_blockNumber', 'horizon_label']

    for pool_flag, df2 in dict2.items():
        df1_prepared = prepare_df_for_comparison(df1, pool_flag, standard_cols)
        df2_prepared = df2 if pool_flag == 'base' else df2.drop(columns=['horizon', 'horizon_label'])

        if not df1_prepared.equals(df2_prepared):
            comparison_result = df1_prepared.compare(df2_prepared)
            raise ValueError(f'Dataframes not equal for pool {pool_flag}:\n{comparison_result}')



def validator_dex_horizons(df1, dict2):
    """
    Validates that the DEX horizons in df1 and df2 are the same.

    Parameters:
    df1 (pd.DataFrame): First dataframe to compare.
    df2 (pd.DataFrame): Second dataframe to compare.

    Returns:
    None. Raises exception if any of the comparisons fail.
    """
    def prepare_df_for_comparison(df, pool_flag, standard_cols):
        standard_cols_extended = standard_cols.copy()
        extended = ['volume_500', 'volume_3000', 'cum_volume_500', 'cum_volume_3000']
        standard_cols_extended.extend(extended)

        if pool_flag == 'base':
            return df[standard_cols_extended]
        else:
            dups = ['min_flag', 'reference_blockNumber']
            cols = [f"{x}_{pool_flag}" for x in dups]
            cols.insert(0, 'blockNumber')
            cols.insert(1, 'horizon')
            cols.extend(['horizon_label', 'closest_blockNumber'])
            cols.extend(extended)
            
            # cols.extend(keep)
            # keep_cols = [x for x in cols if x not in dups]


            df_subset = df[cols]
            df_subset.columns = [x for x in standard_cols_extended]# if x not in ['closest_blockNumber']]
            return df_subset

    standard_cols = ['blockNumber', 'horizon', 'min_flag', 'reference_blockNumber', 'horizon_label', 'closest_blockNumber']

    for pool_flag, df2 in dict2.items():
        df1_prepared = prepare_df_for_comparison(df1, pool_flag, standard_cols)
        df2_prepared = df2 #.drop(columns=['horizon_label'])
        # df2_prepared = df2 if pool_flag == 'base' else df2.drop(columns=['horizon', 'horizon_label'])

        if not df1_prepared.equals(df2_prepared):
            comparison_result = df1_prepared.compare(df2_prepared)
            raise ValueError(f'Dataframes not equal for pool {pool_flag}:\n{comparison_result}')




interim_results_dir = "Data/interim_results"
binance_filepath = "Data/cleansed/binance.csv"

# Load DEX and CEX data
# DEX is at transaction_type // block level
# CEX is at timestamp level
df_dex = pd.read_csv(os.path.join(interim_results_dir, "df_reduced.csv"))

# Calculate the horizons for the reduced DataFrame from the DEX
df_horizons = calculate_horizons(df_dex, step=10)
dict_horizons = calculate_horizons_v02(df_dex, step=10, pool_flags=[500, 3000])
assert validator_horizons(df_horizons, dict_horizons) is None
#Returns -> pools = ['base', '500', '3000']

# Organise DEX data on horizons
df_dex_horizons = organize_dex_data_on_horizons(df_dex, df_horizons)
dict_dex_horizons = organize_dex_data_on_horizons_v02(df_dex, dict_horizons)
# assert validator_dex_horizons(df_dex_horizons, dict_dex_horizons) is None

# # Organize CEX data on horizons
# df_cex = pd.read_csv(os.path.join(binance_filepath))#, nrows=100000)
# df_cex['time'] = pd.to_datetime(df_cex['time'])
# block_times_map = df_dex[['blockNumber', 'timestamp']].drop_duplicates().set_index('timestamp').to_dict()
# dict_cex_horizons = organize_cex_data_on_horizons(df_cex, dict_horizons, block_times_map)

pass















# write to csv
df_horizons.to_csv(os.path.join(interim_results_dir, "df_horizons.csv"), index=False)









# Organise CEX data on horizons
df_cex_horizons = organize_cex_data_on_horizons(df_reduced, df_horizons)

pass



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

