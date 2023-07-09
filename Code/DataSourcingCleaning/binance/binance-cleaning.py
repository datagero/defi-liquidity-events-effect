import pandas as pd

# Uniform Distribution of Volume and Trade Counts: In decentralized exchanges, the volume doesn't necessarily happen at regular intervals but can occur at any time within the blocks. However, your Binance data is at a minutely frequency. To bridge this discrepancy, you are uniformly spreading the volume and trade counts over each second of the minute. This will create a second-by-second approximation of the trade data that would align better with the DEX dataset when they are combined.

# Including the Latest Mid-Price: Including the latest mid-price at each second is important because the price of a trading pair can fluctuate rapidly. By having the mid-price at each second, you can better track price movements and identify potential arbitrage opportunities which can trigger volume spikes.

# Mapping Transactions to Future Block Numbers: Blockchain data is unique because every transaction is associated with a block. However, blocks are not mined at regular intervals, and this is especially true for Ethereum where the block time has changed due to network upgrades. Mapping transactions on Binance to the closest future block number allows you to associate a block with each transaction. This is crucial for creating a dataset that takes into account the idiosyncrasies of blockchain data.

# Avoiding Forward-Looking Bias: By defining the clock of your prediction model via mint operations and considering a strict lower bound whenever computing features after the tick of the clock, you are avoiding introducing forward-looking bias into your model. This means that at any given point in time, the features you use for prediction are only based on information that was available at that time, and not on any future information. This is crucial for creating a model that can generalize well to unseen data.


# Read binance/ etherscan/ and uniswap/ data 
df = pd.read_csv("Data/binance.csv")
# etherscan = pd.read_csv("Data/WBTC-WETH_etherscan.csv")
# uniswap = pd.read_csv("Data/WBTC-WETH.csv")

# Convert time columns to datetime format
df['time'] = pd.to_datetime(df['time'], unit='ms').dt.tz_localize('UTC')

#Uniformly spreading the volume and trade counts over the previous 60 seconds
df.set_index('time', inplace=True)
df_resampled = df.resample('S').first().ffill().reset_index()
df_resampled['qty'] = df_resampled['qty'].div(60)
df_resampled['quoteQty'] = df_resampled['quoteQty'].div(60)


#Include the latest mid-price registered by the exchange at each second:
# assuming 'price' is your mid-price
df['mid_price'] = df['price']
df_resampled = df.resample('S').first().ffill().reset_index()

df_resampled.to_csv('Data/cleansed/binance.csv', index=False)

pass