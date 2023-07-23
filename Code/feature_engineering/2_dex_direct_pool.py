import os
import pickle
import pandas as pd
from tqdm import tqdm
from utils.calculations import *
from utils.common_functions import merge_nested_dicts

"""
This script processes data from different pools of a decentralized exchange (DEX) and computes various metrics per transaction hash and interval.
It loads previously computed interval dataframes, applies various calculations to extract insights (such as trade volume, trade count, and volatility),
and consolidates the results into a single dataframe per pool. If run directly, the processed data can be saved into a CSV file.
It is part of a larger system for analyzing DEX data.
"""


class DEX_DirectPoolProcessor:
    def __init__(self, results_dir="DataIterim/interim_results", write=True, sample=False):
        self.results_dir = results_dir
        self.interval_dataframes = self.load_interval_dataframes()
        self.write = write
        self.sample = sample

    def load_interval_dataframes(self):
        """
        Load previously computed interval dataframes from a pickle file.
        """
        pickle_filepath = os.path.join(self.results_dir, "interval_dataframes.pickle")
        with open(pickle_filepath, "rb") as pickle_file:
            interval_dataframes = pickle.load(pickle_file)
        return interval_dataframes

    def process_data(self, intervals_dict, pool_type, pool_label=''):
        """
        Process the intervals dictionary and calculate metrics for each hash and interval.

        Args:
            intervals_dict (dict): A dictionary containing intervals data for each hash.
                Example:
                {
                    'hash1': {
                        '0': {'df': DataFrame, 'blockTime': datetime},
                        '1': {'df': DataFrame, 'blockTime': datetime},
                        ...
                    },
                    'hash2': {
                        '0': {'df': DataFrame, 'blockTime': datetime},
                        '1': {'df': DataFrame, 'blockTime': datetime},
                        ...
                    },
                    ...
                }
            pool_type (str): The type of pool, either 'same' or 'other'.

        Returns:
            dict: A nested dictionary containing the processed data for each hash and interval.
                Example:
                {
                    'hash1': {
                        's0': 1234,
                        'w0': 5678,
                        'blsame_1': datetime,
                        'slsame_1': 2345,
                        'wlsame_1': 6789,
                        ...
                    },
                    'hash2': {
                        's0': 9876,
                        'w0': 5432,
                        'blsame_1': datetime,
                        'slsame_1': 6789,
                        'wlsame_1': 1234,
                        ...
                    },
                    ...
                }
        """

        # Initialize an empty dictionary to store the processed data
        data = {}

        # Iterate over each hash and its corresponding intervals dictionary
        for hash, intervals_dict in tqdm(intervals_dict.items(), desc=f'{pool_label}::Processing Hashes ({pool_type})'):
            # If the hash is not already in the data dictionary, add it as a new key with an empty nested dictionary
            if hash not in data:
                data[hash] = {}

            # Create an empty DataFrame to store the hash-specific data
            hash_df = pd.DataFrame()

            # Find the maximum interval value from the keys in the intervals dictionary
            max_interval = max([int(x) for x in intervals_dict.keys()])

            # Iterate over each interval and its corresponding interval dictionary
            for interval, interval_dict in intervals_dict.items():
                # Convert the interval label to integer and create label-related strings
                lbl = int(interval)
                lbl_to_next = f"{lbl}{lbl+1}"
                lbl_from_root = f"0_{lbl+1}"

                # Extract the DataFrame for the interval and the block time from the interval dictionary
                df_interval = interval_dict['df']
                block_time = interval_dict['blockTime']

                # Concatenate the interval DataFrame with the hash-specific DataFrame
                hash_df = pd.concat([hash_df, df_interval])

                # Calculate the required metrics for the interval
                sl = get_size(df_interval[df_interval['transaction_type'] == 'mints'], 'size')
                wl = get_width(df_interval[df_interval['transaction_type'] == 'mints'], 'width')
                rateUSD = calculate_traded_volume_rate(df_interval, 'timestamp','amountUSD')
                rateCount = calculate_trades_count(df_interval)
                avgUSD = calculate_average_volume(df_interval, 'amountUSD')

                # Process data unique to the 'same' pool type
                if pool_type == 'same':
                    # Unique to same pool
                    if lbl == 0:
                        # Add size (sl) and width (wl) as keys to the hash's nested dictionary
                        data[hash].update({
                            f's0': sl,
                            f'w0': wl
                        })
                    if lbl != max_interval:
                        # Calculate volatility for non-zero labels and add it as a key to the hash's nested dictionary
                        vol = calculate_volatility(hash_df, 'pool_price')
                        data[hash].update({
                            f'vol_{lbl_from_root}': vol,
                        })

                # Process data common to both pool types
                if lbl != 0:
                    # Add block time (block_time), size (sl), and width (wl) as keys to the hash's nested dictionary
                    data[hash].update({
                        f'bl{pool_type}_{lbl}': block_time,
                        f'sl{pool_type}_{lbl}': sl,
                        f'wl{pool_type}_{lbl}': wl,
                    })

                # If it's not the last label, calculate the metrics and add them as keys to the hash's nested dictionary
                if lbl != max_interval:
                    data[hash].update({
                        f'rate-USD-i{pool_type}_{lbl_to_next}': rateUSD,
                        f'rate-count-i{pool_type}_{lbl_to_next}': rateCount,
                        f'avg-USD-i{pool_type}_{lbl_to_next}': avgUSD,
                    })

        # Return the processed data dictionary
        return data


    def process_pool_data(self, interval_dataframes, sample=False, pool_label=''):
        """
        Process the interval dataframes and generate a DataFrame with the calculated metrics for each pool.
        Args:
            interval_dataframes (dict): A dictionary containing the interval dataframes for the same and other pools.
        Returns:
            DataFrame: A DataFrame containing the calculated metrics for each pool and hash.
        """

        if sample:
            hashes = list(interval_dataframes['same'].keys())[:10]
            interval_dataframes['same'] = {k: v for k, v in interval_dataframes['same'].items() if k in hashes}
            hashes = list(interval_dataframes['other'].keys())[:10]
            interval_dataframes['other'] = {k: v for k, v in interval_dataframes['other'].items() if k in hashes}

        same_pool_data = self.process_data(interval_dataframes['same'], 'same', pool_label)
        other_pool_data = self.process_data(interval_dataframes['other'], 'other', pool_label)

        data = merge_nested_dicts(same_pool_data, other_pool_data)

        df_direct_pool = pd.DataFrame.from_dict(data, orient='index')

        return df_direct_pool

    def main(self):
        """
        The main function of the script. Processes interval dataframes and calculates
        a set of metrics for each pool.
        """
        df_direct_pool = pd.DataFrame()

        for pool in self.interval_dataframes:
            interval_dataframes_pool = self.interval_dataframes[pool]
            df_direct_pool_interim = self.process_pool_data(interval_dataframes_pool, sample=self.sample, pool_label=pool)
            df_direct_pool = pd.concat([df_direct_pool, df_direct_pool_interim], axis=0)


        # Assert that hashids in the dataframe are unique
        assert len(df_direct_pool.index.unique()) == len(df_direct_pool.index), "Hashids are not unique"

        # Define order of columns for final dataframe
        order_cols = ['s0', 'w0',
                    'blsame_1', 'slsame_1', 'wlsame_1',
                    'blsame_2', 'slsame_2', 'wlsame_2',
                    'blsame_3', 'slsame_3', 'wlsame_3',
                    'vol_0_1', 'vol_0_2', 'vol_0_3',
                        'rate-USD-isame_01', 'rate-USD-isame_12', 'rate-USD-isame_23',
                        'rate-count-isame_01', 'rate-count-isame_12', 'rate-count-isame_23',
                        'avg-USD-isame_01', 'avg-USD-isame_12', 'avg-USD-isame_23',
                    'blother_1', 'slother_1', 'wlother_1',
                    'blother_2', 'slother_2', 'wlother_2',
                    'blother_3', 'slother_3', 'wlother_3',
                        'rate-USD-iother_01', 'rate-USD-iother_12', 'rate-USD-iother_23',
                        'rate-count-iother_01', 'rate-count-iother_12', 'rate-count-iother_23',
                        'avg-USD-iother_01', 'avg-USD-iother_12', 'avg-USD-iother_23'
                ]

        # Assert that all expected columns are present in the dataframe
        assert set(order_cols) == set(df_direct_pool.columns), "Columns are not the same " + str(set(order_cols).difference(set(df_direct_pool.columns))) + str(set(df_direct_pool.columns).difference(set(order_cols)))

        # Reorder dataframe columns
        df_direct_pool = df_direct_pool[order_cols]

        # Write resulting dataframe to csv file if write=True
        if self.write:
            df_direct_pool.to_csv('Data/processed/direct_pool.csv')

# Only run main function if script is run directly (not imported)
if __name__ == '__main__':
    processor = DEX_DirectPoolProcessor(write=True, sample=False)
    processor.main()