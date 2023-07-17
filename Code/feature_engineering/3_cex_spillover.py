import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from utils.build_intervals import create_interval_dataframes

class CEX_SpilloverProcessor:
    def __init__(self, interim_results_dir="Data/interim_results", binance_filepath="Data/cleansed/binance.csv"):
        self.interim_results_dir = interim_results_dir
        self.binance_filepath = binance_filepath

    def _get_block_times_map(self, df_blocks_full):
        return df_blocks_full[['blockNumber', 'timestamp']].drop_duplicates().set_index('timestamp').to_dict()

    def _import_data(self, sample):
        df_blocks_full = pd.read_csv(os.path.join(self.interim_results_dir, "df_blocks_full.csv"))
        block_times_map = self._get_block_times_map(df_blocks_full)
        pool_flags = list(df_blocks_full['pool'].unique())

        if sample:
            df_cex = pd.read_csv(os.path.join(self.binance_filepath), nrows=100000)
        else:
            df_cex = pd.read_csv(os.path.join(self.binance_filepath))

        df_cex['time'] = pd.to_datetime(df_cex['time'])
        df_cex['closest_blockNumber'] = pd.Series([block_times_map['blockNumber'].get(str(time), None) for time in df_cex['time']]).ffill().fillna(-1).astype(int)

        agg_dict = {
            'CEX_traded_volume_BTC': 'sum',
            'CEX_mid_price': 'mean',
            'CEX_transactions_count': 'sum'
        }

        df_cex_reference = df_cex.groupby('closest_blockNumber').agg(agg_dict).reset_index().rename(columns={'closest_blockNumber': 'blockNumber'})

        df_cex_full = pd.DataFrame()
        for pool_flag in pool_flags:
            df_cex_pool = df_cex_reference.copy(deep=True)
            df_cex_pool['pool'] = pool_flag
            df_cex_full = pd.concat([df_cex_full, df_cex_pool])

        return df_blocks_full, df_cex_full

    def _calculate_metrics(self, df_interval):
        agg_dict = {
            'CEX_traded_volume_BTC': 'sum',
            'CEX_mid_price': 'mean',
            'CEX_transactions_count': 'sum'
        }

        df_cex_reference = df_interval.groupby('blockNumber').agg(agg_dict).reset_index()
        if df_cex_reference.shape[0] > 0:
            count = df_cex_reference['CEX_transactions_count'].values[0]
            volume = df_cex_reference['CEX_traded_volume_BTC'].values[0]
            mid_price = df_cex_reference['CEX_mid_price'].values[0]
        else:
            count = np.nan
            volume = np.nan
            mid_price = np.nan

        return count, volume, mid_price

    def process_data(self, intervals_dict, pool_type, pool_label=''):
        data = {}
        for hash, intervals_dict in tqdm(intervals_dict.items(), desc=f'{pool_label}::Processing Hashes ({pool_type})'):
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

                count, volume, mid_price = self._calculate_metrics(df_interval)

                if lbl != max_interval:
                    data[hash].update({
                        f'binance-count-{lbl_to_next}': count,
                        f'binance-btc-{lbl_to_next}': volume,
                        f'binance-midprice-{lbl_to_next}': mid_price,
                    })
        return data

    def run(self, write=False, sample=True):
        df_blocks_full, df_cex_full = self._import_data(sample)
        interval_dataframes = create_interval_dataframes(df_blocks_full, df_cex_full, 'pool')
        df_cex_spillovers = pd.DataFrame()

        for pool in interval_dataframes:
            interval_dataframes_pool = interval_dataframes[pool]
            cex_spillovers_same_pool = self.process_data(interval_dataframes_pool['same'], 'same', pool_label=pool)
            df_cex_spillovers_pool = pd.DataFrame.from_dict(cex_spillovers_same_pool, orient='index')
            df_cex_spillovers = pd.concat([df_cex_spillovers, df_cex_spillovers_pool], axis=0)

        assert len(df_cex_spillovers.index.unique()) == len(df_cex_spillovers.index), "Hashids are not unique"

        order_cols = ['binance-count-01', 'binance-btc-01', 'binance-midprice-01', 'binance-count-12', 'binance-btc-12', 'binance-midprice-12', 'binance-count-23', 'binance-btc-23', 'binance-midprice-23']
        assert set(order_cols) == set(df_cex_spillovers.columns), "Columns are not the same " + str(set(order_cols).difference(set(df_cex_spillovers.columns))) + str(set(df_cex_spillovers.columns).difference(set(order_cols)))

        df_cex_spillovers = df_cex_spillovers[order_cols]
        if write:
            df_cex_spillovers.to_csv('Data/processed/cex_spillovers.csv')

if __name__ == '__main__':
    processor = CEX_SpilloverProcessor()
    processor.run(write=True, sample=False)
