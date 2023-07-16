import matplotlib.pyplot as plt
import pandas as  pd
import statsmodels.api as sm

reference_pool_mints = [500, 3000]
reference_pool = 500

# Read df_features_raw
df = pd.read_csv(f"Data/processed/df_features_raw_ref{reference_pool}.csv")

#all_cols = df.columns.tolist()
#non_features = ['blockNumber', 'horizon', 'min_flag', 'closest_blockNumber', 'volume_500', 'volume_3'hashid', 'pool', 'reference_blockNumber', 'horizon_label', 'cum_volume_500', 'cum_volume_3000', 'cum_volume_3000_ref500']

# Null analysis
null_counts = df.isnull().sum()
null_counts.sort_values(ascending=False, inplace=True)
print(null_counts[:10])

target_variable = 'cum_volume_3000'
explainable_variables = ['s0', 'w0', 'blsame_1', 'slsame_1', 'wlsame_1', 'blsame_2', 'slsame_2', 'wlsame_2', 'blsame_3', 'slsame_3', 'wlsame_3', 'vol_0_1', 'vol_0_2', 'vol_0_3', 'rate-USD-isame_01', 'rate-USD-isame_12', 'rate-USD-isame_23', 'rate-count-isame_01', 'rate-count-isame_12', 'rate-count-isame_23', 'avg-USD-isame_01', 'avg-USD-isame_12', 'avg-USD-isame_23', 'blother_1', 'slother_1', 'wlother_1', 'blother_2', 'slother_2', 'wlother_2', 'blother_3', 'slother_3', 'wlother_3', 'rate-USD-iother_01', 'rate-USD-iother_12', 'rate-USD-iother_23', 'rate-count-iother_01', 'rate-count-iother_12', 'rate-count-iother_23', 'avg-USD-iother_01', 'avg-USD-iother_12', 'avg-USD-iother_23', 'binance-count-01', 'binance-btc-01', 'binance-midprice-01', 'binance-count-12', 'binance-btc-12', 'binance-midprice-12', 'binance-count-23', 'binance-btc-23', 'binance-midprice-23']


# Split the data into target variable and explanatory variables
X = df[explainable_variables]
y = df[target_variable]

# Remove the columns that are not needed or pending further cleaning
print("Current row count:", len(X))
remove = ['vol_0_1', 'vol_0_2', 'vol_0_3', 'avg-USD-iother_01', 'rate-USD-iother_01']
X = X.drop(remove, axis=1)

# Remove the rows that contain null values
null_rows = X[X.isnull().any(axis=1)].index
X = X.drop(null_rows, axis=0)
y = y.drop(null_rows, axis=0)
print("New row count:", len(X))

# Print difference in row count
data_loss_percentage = ((len(df) - len(X)) / len(df)) * 100
print("Difference in row count:", len(df) - len(X))
print('Data loss as a percentage: %.2f%%' % data_loss_percentage)


# Limit the data to the first 30 horizons (i.e., 300 blocks) The average block time for Ethereum is approximately 13-15 seconds. 300*15=4500 seconds = 75 minutes
df_filtered = df[df['horizon_label'] <= 30]
horizon_labels = df_filtered['horizon_label'].unique() 

r_squared_values = []
horizon_values = []
observation_counts = []


for horizon in horizon_labels:
    subset_data = df_filtered[df_filtered['horizon_label'] == horizon]
    X = subset_data[explainable_variables]
    y = subset_data[target_variable]

    # Remove the columns that are not needed or pending further cleaning
    # print("===========", horizon, "===========")
    # print("Current row count:", len(X))
    remove = ['vol_0_1', 'vol_0_2', 'vol_0_3', 'avg-USD-iother_01', 'rate-USD-iother_01']
    X = X.drop(remove, axis=1)

    # Remove the rows that contain null values
    null_rows = X[X.isnull().any(axis=1)].index
    X = X.drop(null_rows, axis=0)
    y = y.drop(null_rows, axis=0)
    # print("New row count:", len(X))

    # Fit the OLS model
    model = sm.OLS(y, X)
    results = model.fit()

    # Retrieve the R-squared value
    r_squared = results.rsquared
    
    r_squared_values.append(r_squared)
    horizon_values.append(horizon * 10)
    observation_counts.append(len(X))

    if horizon == 20:
        print(results.summary())


fig, ax1 = plt.subplots()
ax2 = ax1.twinx()

ax1.scatter(horizon_values, r_squared_values, color='b')
ax1.set_xlabel('Horizon (blocks)')
ax1.set_ylabel('R-squared', color='b')
ax1.set_ylim(0, 1)  # Set the y-axis range for R-squared

ax2.plot(horizon_values, observation_counts, color='r')
ax2.set_ylabel('Observation Count', color='r')
ax2.set_ylim(0, max(observation_counts) + 1000)  # Set the y-axis range for observation count

plt.title('R-squared and Observation Count vs. Horizon')
plt.suptitle(f'Mint of reference on Pool: {reference_pool}')
plt.show()



pass