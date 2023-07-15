import pandas as pd
from tqdm import tqdm

def uniform_distribution(df, time_column, agg_dict, rename_dict=None, interval='S'):
    """
    Perform uniform distribution of values over time intervals in a DataFrame.

    # Uniform Distribution of Volume and Trade Counts:
    In decentralized exchanges, the volume doesn't necessarily happen at regular intervals but can occur at any time within the blocks.
    However, Binance data is at a minutely frequency.
    To bridge this discrepancy, we uniformly spread the volume and trade counts over each second of the minute.
    This will create a second-by-second approximation of the trade data that would align better with the DEX dataset when they are combined.

    """
    print("Resampling and aggregating data...")
    df_resampled = df.set_index(time_column).resample(interval).agg(agg_dict).ffill().reset_index()
    if rename_dict:
        print("Renaming columns...")
        df_resampled.rename(columns=rename_dict, inplace=True)

    return df_resampled

def main(sample=False):
    # Read binance data
    if sample:
        print("Reading a sample of the data...")
        df = pd.read_csv("Data/binance.csv", nrows=100)
    else:
        print("Reading the full data...")
        df = pd.read_csv("Data/binance.csv")

    # Convert time column to datetime format and localize to UTC
    df['time'] = pd.to_datetime(df['time'], unit='ms').dt.tz_localize('UTC')

    # Calculate the trading volume in BTC for each trade
    # Suppose you are trading the trading pair ETH/BTC, where BTC is the base asset, and ETH is the quote asset.
    # Price is quoted on the price of one ETH in terms of BTC.
    df['traded_volume_BTC'] = df['quoteQty'] * df['price']

    # Uniformly spread the volume and trade counts over the previous 60 seconds
    agg_dict = {
        'traded_volume_BTC': 'sum',
        'price': 'mean',
        'id': 'count'
    }

    rename_dict = {
        'traded_volume_BTC': 'CEX_traded_volume_BTC',
        'price': 'CEX_mid_price',
        'id': 'CEX_transactions_count'
    }

    # Note -> By having the mid-price at each second, we can better track price movements and identify potential arbitrage opportunities which can trigger volume spikes.
    df_uniform = uniform_distribution(df, 'time', agg_dict, rename_dict=rename_dict)

    chunk_size = 1000  # Set the desired chunk size

    print("Saving the uniform data...")
    with tqdm(total=len(df_uniform)) as pbar:
        for i, chunk in enumerate(df_uniform.groupby(df_uniform.index // chunk_size)):
            chunk_df = chunk[1]
            if i == 0:
                chunk_df.to_csv('Data/cleansed/binance.csv', index=False)
            else:
                chunk_df.to_csv('Data/cleansed/binance.csv', index=False, header=False, mode='a')
            pbar.update(len(chunk_df))
    print("binance data cleansing and aggregation completed.")


if __name__ == '__main__':
    main()


# Further information
'''
# Mapping Transactions to Future Block Numbers:

This step is done on a separate script as is dependend on some data engineering activities, but important to keep in mind here.
Blockchain data is unique because every transaction is associated with a block.
However, blocks are not mined at regular intervals, and this is especially true for Ethereum where the block time has changed due to network upgrades.
Mapping transactions on Binance to the closest future block number allows to associate a block with each transaction.
This is crucial for creating a dataset that takes into account the idiosyncrasies of blockchain data.

# Avoiding Forward-Looking Bias:

This step is a modelling consideration, important to keep in mind here
By defining the clock of the prediction model via mint operations and considering a strict lower bound whenever computing features after the tick of the clock,
we are avoiding introducing forward-looking bias into the models.
This means that at any given point in time, the features used for prediction are only based on information that was available at that time, and not on any future information.
This is crucial for creating a model that can generalize well to unseen data.
'''




