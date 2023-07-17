import pandas as pd
import numpy as np

def calculate_horizons(df, step, pool_flags):
    def get_expanded_series(df_mints):
        block_numbers = df_mints['blockNumber'].sort_values().values
        expanded_blocks = [np.arange(start, end, step) for start, end in zip(block_numbers[:-1], block_numbers[1:])]
        return pd.concat([pd.Series(np.concatenate(expanded_blocks)), pd.Series(block_numbers[-1])])
    
    def set_horizon_data(df_horizons, pool_flag):
        # Calculate the difference between consecutive block numbers and fill missing values with 0
        # Set min_flag to 1 if the blockNumber is present in df_mints, otherwise set it to 0
        # Set the reference_blockNumber to the blockNumber when min_flag is 1, and forward fill missing values
        # Group the DataFrame by reference_blockNumber and calculate the cumulative count within each group, starting from 1

        df_horizons = df_horizons.copy()

        min_flag_key = 'min_flag'
        reference_block_key = 'reference_blockNumber'
        horizon_label_key = 'horizon_label'
        
        if pool_flag == 'base':
            df_horizons[min_flag_key] = df_horizons['blockNumber'].isin(df_mints['blockNumber']).astype(int)
        else:
            df_horizons[min_flag_key] = df_horizons['blockNumber'].isin(df_mints[df_mints['pool'] == int(pool_flag)]['blockNumber']).astype(int)
    
        df_horizons[reference_block_key] = pd.Series(np.where(df_horizons[min_flag_key] == 1, df_horizons['blockNumber'], np.nan)).ffill().fillna(-1).astype(int)
        df_horizons[horizon_label_key] = df_horizons.groupby(reference_block_key).cumcount() + 1

        return df_horizons.reset_index(drop=True)

    df_mints = df[df['transaction_type'] == 'mints']
    df_horizons = pd.DataFrame(get_expanded_series(df_mints), columns=['blockNumber'])
    df_horizons['horizon'] = df_horizons['blockNumber'].diff().fillna(0)

    # Default flag for all transactions
    horizons_dict = {}
    horizons_dict['base'] = set_horizon_data(df_horizons, 'base')

    # Set up horizon data for each pool flag
    for pool_flag in pool_flags:
        flag_key = str(pool_flag)
        horizons_dict[flag_key] = set_horizon_data(df_horizons, flag_key)

    return horizons_dict


def organize_target_data_on_horizons(df, df_horizons):
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

        #Add base cumulative volume
        cum_volume_cols = [col for col in df_merged.columns if 'cum_volume_' in col]
        df_merged['cum_volume_base'] = df_merged[cum_volume_cols].sum(axis=1)
    
        organized_data_dict[flag_key] = df_merged

    return organized_data_dict
