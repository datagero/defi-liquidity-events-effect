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
