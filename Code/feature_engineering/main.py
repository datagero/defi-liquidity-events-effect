import os
import pandas as pd
from utils.horizon_aggregates import calculate_horizons, organize_target_data_on_horizons

interim_results_dir = "DataIterim/interim_results"
processed_results_dir = "Data/processed"

def load_data(path, filename):
    """
    Load a CSV file from a directory
    """
    return pd.read_csv(os.path.join(path, filename), index_col=0)

def get_blocks():
    """
    Load blocks data and filter it for relevant columns
    """
    df_blocks_full = load_data(interim_results_dir, "df_blocks_full.csv")
    df_blocks = df_blocks_full[['blockNumber', 'pool']]
    df_blocks = df_blocks.rename(columns={'blockNumber': 'reference_blockNumber'})
    df_blocks['reference_blockNumber'] = df_blocks['reference_blockNumber'].astype(int)
    return df_blocks

def calculate_and_organize_horizons(df):
    """
    Calculate horizons and organize data on horizons
    """
    dict_horizons = calculate_horizons(df, step=10, pool_flags=[500, 3000])
    dict_target_horizons = organize_target_data_on_horizons(df, dict_horizons)
    return dict_target_horizons

def expand_horizons_with_features(df, df_blocks):
    """
    Expand horizons with direct pool or dex spillover features (or others...)
    """
    return df.merge(df_blocks, how='left', left_index=True, right_on='hashid')

def compute_data_loss_percentage(old_len, new_len):
    """
    Compute data loss percentage
    """
    return ((old_len - new_len) / old_len) * 100

def process_pool(pool, dict_target_horizons, df_direct_pool_blocks, df_cex_spillovers_blocks):
    """
    Process each pool
    """
    print('=====================','\n', 'Processing pool: ', pool, '\n', '=====================')
    dict_dex_horizons_reference = dict_target_horizons[str(pool)]

    df_direct_pool_blocks_reference = df_direct_pool_blocks[df_direct_pool_blocks['pool'] == pool] if pool != 'base' else df_direct_pool_blocks
    df_cex_spillovers_blocks_reference = df_cex_spillovers_blocks[df_cex_spillovers_blocks['pool'] == pool] if pool != 'base' else df_cex_spillovers_blocks

    df_dex_features = dict_dex_horizons_reference.merge(df_direct_pool_blocks_reference, how='inner', on='reference_blockNumber')
    data_loss_percentage = compute_data_loss_percentage(len(dict_dex_horizons_reference), len(df_dex_features))
    print('Data loss after merge with direct pools: ', len(dict_dex_horizons_reference) - len(df_dex_features))
    print('Data loss as a percentage: %.2f%%' % data_loss_percentage)

    df = df_dex_features.drop(['pool'], axis=1).merge(df_cex_spillovers_blocks_reference, how='inner', on='reference_blockNumber')
    data_loss_percentage = compute_data_loss_percentage(len(df_dex_features), len(df))
    print('Data loss after merge with cex spillovers pools: ', len(df_dex_features) - len(df))
    print('Data loss as a percentage: %.2f%%' % data_loss_percentage)

    df.to_csv(os.path.join(processed_results_dir, 'features', f"df_features_raw_ref{pool}.csv"), index=False)

# Main execution
df_blocks = get_blocks()
df_dex = load_data(interim_results_dir, "df_reduced.csv")
dict_target_horizons = calculate_and_organize_horizons(df_dex)
df_direct_pool_blocks = expand_horizons_with_features(load_data(processed_results_dir, "direct_pool.csv"), df_blocks)
df_cex_spillovers_blocks = expand_horizons_with_features(load_data(processed_results_dir, "cex_spillovers.csv"), df_blocks)

unique_pools = list(df_blocks['pool'].unique())
unique_pools.insert(0, 'base')

for pool in unique_pools:
    process_pool(pool, dict_target_horizons, df_direct_pool_blocks, df_cex_spillovers_blocks)
