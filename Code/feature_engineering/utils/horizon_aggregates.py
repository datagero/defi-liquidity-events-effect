import pandas as pd
import numpy as np

def organize_dex_data_on_horizons(df, df_horizons):
    # Calculate cummulative incoming volume
    # For now, assume only for swaps.
    df_swaps = df[df['transaction_type']=='swaps']
    df_grouped = df_swaps.groupby(['blockNumber', 'pool'])['amountUSD'].sum().reset_index()
    df_pivoted = df_grouped.pivot(index='blockNumber', columns=['pool'], values='amountUSD')
    df_pivoted.columns = ['volume_500', 'volume_3000']
    df_pivoted = df_pivoted.reset_index()

    # Get closest blockNumber from the base table
    df_reference_sorted = df_horizons.sort_values('blockNumber')
    indices = df_reference_sorted['blockNumber'].searchsorted(df_pivoted['blockNumber'], side='right') - 1
    clipped_indices = np.clip(indices, 0, len(df_reference_sorted) - 1)
    df_pivoted['closest_blockNumber'] = df_reference_sorted.loc[clipped_indices, 'blockNumber'].values
    df_pivoted['closest_blockNumber'] = df_pivoted['closest_blockNumber'].astype(int)

    # Aggregate sum incoming volume into the closest reference_blockNumber
    df_volumes = df_pivoted.groupby('closest_blockNumber').sum().reset_index().drop(columns=['blockNumber'])

    # Merge with base expanded dataframe for a complete view
    df_merged = df_horizons.merge(df_volumes, left_on='blockNumber', right_on='closest_blockNumber', how='left')

    df_merged[['volume_500', 'volume_3000']] = df_merged[['volume_500', 'volume_3000']].fillna(0)

    # Create cumulative volume columns for each reference_blockNumber
    df_merged['cum_volume_500'] = df_merged.groupby('reference_blockNumber')['volume_500'].cumsum()
    df_merged['cum_volume_3000'] = df_merged.groupby('reference_blockNumber')['volume_3000'].cumsum()

    df_merged['cum_volume_500_ref500'] = df_merged.groupby('reference_blockNumber_500')['volume_500'].cumsum()
    df_merged['cum_volume_3000_ref500'] = df_merged.groupby('reference_blockNumber_500')['volume_3000'].cumsum()

    df_merged['cum_volume_500_ref3000'] = df_merged.groupby('reference_blockNumber_3000')['volume_500'].cumsum()
    df_merged['cum_volume_3000_ref3000'] = df_merged.groupby('reference_blockNumber_3000')['volume_3000'].cumsum()

    return df_merged


def organize_target_data_on_horizons_v02(df, df_horizons):
    """
    Organizes the DEX data according to the specified horizons.

    Parameters:
    df (pd.DataFrame): Input dataframe containing transaction data. 
                       Expected to have 'transaction_type', 'blockNumber', and 'pool' columns.
    df_horizons (dict): Dictionary containing horizons dataframes for base and specified pool flags.
    pool_flags (list): List of pool flags for which to organize data.

    Returns:
    dict: Dictionary containing organized DEX data for base and specified pool flags.
    """

    # We get the pool flags from horizons, except for base flag
    pool_flags = [pool_flag for pool_flag in df_horizons.keys() if pool_flag != 'base']
    assert len(pool_flags) == 2, "Only 2 pool flags are supported, 'same', and 'other'."

    # Calculate cummulative incoming volume for swaps
    df_swaps = df[df['transaction_type']=='swaps']
    df_grouped = df_swaps.groupby(['blockNumber', 'pool'])['amountUSD'].sum().reset_index()
    df_pivoted = df_grouped.pivot(index='blockNumber', columns=['pool'], values='amountUSD')
    df_pivoted.columns = [f'volume_{pool_flag}' for pool_flag in pool_flags]
    df_pivoted = df_pivoted.reset_index()

    organized_data_dict = {}

    for flag_key, df_horizon in df_horizons.items():
        # Get closest blockNumber from the horizon table
        df_reference_sorted = df_horizon.sort_values('blockNumber')
        indices = df_reference_sorted['blockNumber'].searchsorted(df_pivoted['blockNumber'], side='right') - 1
        clipped_indices = np.clip(indices, 0, len(df_reference_sorted) - 1)
        df_pivoted['closest_blockNumber'] = df_reference_sorted.loc[clipped_indices, 'blockNumber'].values
        df_pivoted['closest_blockNumber'] = df_pivoted['closest_blockNumber'].astype(int)

        # Aggregate sum incoming volume into the closest reference_blockNumber
        df_volumes = df_pivoted.groupby('closest_blockNumber').sum().reset_index().drop(columns=['blockNumber'])

        # Merge with base expanded dataframe for a complete view
        df_merged = df_horizon.merge(df_volumes, left_on='blockNumber', right_on='closest_blockNumber', how='left')
        df_merged[[f'volume_{pool_flag}' for pool_flag in pool_flags]] = df_merged[[f'volume_{pool_flag}' for pool_flag in pool_flags]].fillna(0)

        # Create cumulative volume columns for each reference_blockNumber
        for pool_flag in pool_flags:
            volume_col = f'volume_{pool_flag}'
            cum_volume_col = f'cum_volume_{pool_flag}'
            reference_block_col = f'reference_blockNumber'

            df_merged[cum_volume_col] = df_merged.groupby(reference_block_col)[volume_col].cumsum()

        organized_data_dict[flag_key] = df_merged

    return organized_data_dict

def organize_cex_data_on_horizons(df, df_horizons, block_times_map):
    """
    Organise it on reference block number.
    The same record with then extrapolate to the steps.
    Note that this approach differs from organize_dex_data_on_horizons_v02 and in a way is more similar to how the dex data is joined back to horizons before the modelling step.
    It is kept on horizon aggregates as we still have to engineer the time to block number mapping.
    """

    # We get the pool flags from horizons, except for base flag
    pool_flags = [pool_flag for pool_flag in df_horizons.keys() if pool_flag != 'base']
    assert len(pool_flags) == 2, "Only 2 pool flags are supported, 'same', and 'other'."

    for flag_key, df_horizon in df_horizons.items():
        df_references = df_horizon[['blockNumber', 'reference_blockNumber']]
        df_cex = df.copy(deep=True)
        # map reference_blockNumber to timestamp
        df_cex['reference_blockNumber'] = pd.Series([block_times_map['blockNumber'].get(str(time), None) for time in df_cex['time']]).ffill()
        

        agg_dict = {
            'CEX_traded_volume_BTC': 'sum',
            'CEX_mid_price': 'mean',
            'CEX_transactions_count': 'sum'
        }

        df_cex_reference = df_cex.groupby('reference_blockNumber').agg(agg_dict).reset_index()
        



        a = [time for time in df_cex['time']]

        df_cex['time'].map(block_times_map['blockNumber'])

        df_references['timestamp'] = df_references['reference_blockNumber'].map()




        # Get closest blockNumber from the horizon table
        df_reference_sorted = df_horizon.sort_values('blockNumber')
        # indices = df_reference_sorted['blockNumber'].searchsorted(df_pivoted['blockNumber'], side='right') - 1
        # clipped_indices = np.clip(indices, 0, len(df_reference_sorted) - 1)
        # df_pivoted['closest_blockNumber'] = df_reference_sorted.loc[clipped_indices, 'blockNumber'].values
        # df_pivoted['closest_blockNumber'] = df_pivoted['closest_blockNumber'].astype(int)

        # # Aggregate sum incoming volume into the closest reference_blockNumber
        # df_volumes = df_pivoted.groupby('closest_blockNumber').sum().reset_index().drop(columns=['blockNumber'])

        # # Merge with base expanded dataframe for a complete view
        # df_merged = df_horizon.merge(df_volumes, left_on='blockNumber', right_on='closest_blockNumber', how='left')
        # df_merged[[f'volume_{pool_flag}' for pool_flag in pool_flags]] = df_merged[[f'volume_{pool_flag}' for pool_flag in pool_flags]].fillna(0)