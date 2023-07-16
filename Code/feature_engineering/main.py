
import os
import pandas as pd
from utils.build_intervals import calculate_horizons, calculate_horizons_v02
from utils.horizon_aggregates import organize_target_data_on_horizons_v02, organize_cex_data_on_horizons

interim_results_dir = "Data/interim_results"
processed_results_dir = "Data/processed"
binance_filepath = "Data/cleansed/binance.csv"

def get_blocks():
    # Load blocks
    df_blocks_full = pd.read_csv(os.path.join(interim_results_dir, "df_blocks_full.csv"))
    # Filter df_blocks_full to only include the reference pool
    # df_blocks = df_blocks_full[df_blocks_full['pool'] == reference_pool]
    df_blocks = df_blocks_full[['hashid', 'blockNumber', 'pool']]
    # assert df_blocks[df_blocks['blockNumber'].notnull()]['blockNumber'].is_unique
    df_blocks = df_blocks.rename(columns={'blockNumber': 'reference_blockNumber'})
    df_blocks['reference_blockNumber'] = df_blocks['reference_blockNumber'].astype(int)
    return df_blocks


# Load blocks for references and hash mappings
df_blocks = get_blocks()

# Load DEX and CEX data
df_dex = pd.read_csv(os.path.join(interim_results_dir, "df_reduced.csv"))

# Calculate the horizons for the reduced DataFrame from the DEX
dict_horizons = calculate_horizons_v02(df_dex, step=10, pool_flags=[500, 3000])

# Organise DEX-based target data on horizons (i.e., dependent variables)
dict_target_horizons = organize_target_data_on_horizons_v02(df_dex, dict_horizons)

# Expand horizons with direct pool features
# df_direct_pool is at hash level, so need to map it to reference block
df_direct_pool = pd.read_csv(os.path.join(processed_results_dir, "direct_pool.csv"), index_col=0)
df_direct_pool_blocks = df_direct_pool.merge(df_blocks, how='left', left_index=True, right_on='hashid')

# Load CEX data
# df_cex_spillovers is at hash level, so need to map it to reference block
df_cex_spillovers = pd.read_csv(os.path.join(processed_results_dir, "cex_spillovers.csv"), index_col=0)
df_cex_spillovers_blocks = df_cex_spillovers.merge(df_blocks, how='left', left_index=True, right_on='hashid')


unique_pools = list(df_blocks['pool'].unique())
unique_pools.insert(0, 'base')

for pool in unique_pools:

    print('=====================','\n', 'Processing pool: ', pool, '\n', '=====================')
    # Build target horizons for each reference pool
    dict_dex_horizons_reference = dict_target_horizons[str(pool)]

    # build features for each reference pool
    if pool == 'base':
        df_direct_pool_blocks_reference = df_direct_pool_blocks[df_direct_pool_blocks['pool'] == pool]
        df_cex_spillovers_blocks_reference = df_cex_spillovers_blocks[df_cex_spillovers_blocks['pool'] == pool]
    else:
        df_direct_pool_blocks_reference = df_direct_pool_blocks
        df_cex_spillovers_blocks_reference = df_cex_spillovers_blocks

    # Build full features dataframe
    # TODO -> In future analysis, data loss will be analysed further, currently less than 2% data loss, so allowed.
    df_dex_features = dict_dex_horizons_reference.merge(df_direct_pool_blocks, how='inner', on='reference_blockNumber')
    data_loss_percentage = ((len(dict_dex_horizons_reference) - len(df_dex_features)) / len(dict_dex_horizons_reference)) * 100
    print('Data loss after merge with direct pools: ', len(dict_dex_horizons_reference) - len(df_dex_features))
    print('Data loss as a percentage: %.2f%%' % data_loss_percentage)

    df = df_dex_features.drop(['hashid', 'pool'], axis=1).merge(df_cex_spillovers_blocks, how='inner', on='reference_blockNumber')
    data_loss_percentage = ((len(df_dex_features) - len(df)) / len(df_dex_features)) * 100
    print('Data loss after merge with cex spillovers pools: ', len(df_dex_features) - len(df))
    print('Data loss as a percentage: %.2f%%' % data_loss_percentage)

    df.to_csv(os.path.join(processed_results_dir, f"df_features_raw_ref{pool}.csv"), index=False)
