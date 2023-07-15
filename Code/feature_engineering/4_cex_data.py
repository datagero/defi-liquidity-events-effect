import pandas as pd

# Cleansed binance data includes
    # binance data is at minutely frequency, in UTC time
    # binance notional values are quoted in BTC
    # uniformly spread the volume and trade counts for Binance operations recorded at the end of each minute over the previous 60 seconds
    # At each second, we also include the latest mid-price registered by the exchange, since divergences with Uniswap pool prices can trigger volume from arbitrageurs.

# Next, we consider our DEX data set and
# extract all transaction block numbers with the related timestamps. Blocks are mined
# every 10−19 seconds on Ethereum before its upgrade to the Proof of Stake consensus
# mechanism, and every 12 seconds afterwards



df_binance = pd.read_csv("Data/cleansed/binance.csv")
df_reduced = pd.read_csv("Data/interim_results/df_reduced.csv")

# Join on timestamp
df = df_reduced.merge(df_binance, how='inner', left_on='timestamp', right_on='time')


# df_uniswap = pd.read_csv("Data/cleansed/uniswap.csv")


# We require a standard timezone to link block mining times to the latest realised activity on Binance. 

#By comparing transactions data with Etherscan2, we adjust times to the standard UTC
#timezone and restrict ourselves to the period between April and September 2022 to
#avoid misalignment errors due to daylight savings.



df_binance['timestamp'] = pd.to_datetime(df_binance['time']) #.dt.tz_localize('UTC')
# df_uniswap['timestamp'] = pd.to_datetime(df_uniswap['timestamp'], unit='s').dt.tz_localize('UTC')

# df_binance.sort_values(by='timestamp', inplace=True)
# df_uniswap.sort_values(by='timestamp', inplace=True)


#To associate Binance data to block numbers, we first nds
df_binance['blockNumber'] = df_binance['timestamp'].apply(lambda x: int(x.timestamp() - 60) // 60)

df_binance['pool_price'] = df_binance['close']

pass