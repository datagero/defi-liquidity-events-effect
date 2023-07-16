# This script is in progress. It is used for PoC of linear regression on the data as reported on the progress report.


import os
import pandas as pd
import matplotlib.pyplot as plt
from feature_engineering.utils.build_intervals import calculate_horizons
from feature_engineering.utils.horizon_aggregates import organize_dex_data_on_horizons


reference_pool_mint = 500
reference_block_label = 'reference_blockNumber_500'

# Define the directory path where the files are saved
interim_results_dir = "Data/interim_results"
processed_results_dir = "Data/processed"

# Open the CSV files as DataFrames
df_reduced = pd.read_csv(os.path.join(interim_results_dir, "df_reduced.csv"))
df_blocks_full = pd.read_csv(os.path.join(interim_results_dir, "df_blocks_full.csv"))
df_direct_pool = pd.read_csv(os.path.join(processed_results_dir, "direct_pool.csv"), index_col=0)



mints_null_counts = df_direct_pool.isnull().sum()
mints_null_counts.sort_values(ascending=False, inplace=True)

# Calculate the horizons for the reduced DataFrame
df_horizons = calculate_horizons(df_reduced, step=10)
df_horizons = organize_dex_data_on_horizons(df_reduced, df_horizons)

# ## CEX Data
# # Read binance cleansed data
# binance_filepath = "Data/cleansed/binance.csv"
# df_binance = pd.read_csv(binance_filepath)
# df_binance['time'] = pd.to_datetime(df_binance['time'])

## Merge DEX and CEX data
# df_ex = pd.merge(df_direct_pool, df_binance, how='inner', left_on='timestamp', right_on='time')
# print('Data loss after merge with binance: ', len(df_direct_pool) - len(df_ex))





# Filter df_blocks_full to only include the reference pool
df_blocks = df_blocks_full[['hashid', 'blockNumber']].where(df_blocks_full['pool'] == reference_pool_mint)
# Assert unique blockNumber values, after removing null values
assert df_blocks[df_blocks['blockNumber'].notnull()]['blockNumber'].is_unique

# join df_direct_pool with df_blocks
df_blocks = df_blocks.rename(columns={'blockNumber': reference_block_label})
df_reference = df_direct_pool.merge(df_blocks, how='left', left_index=True, right_on='hashid')

# join df with df_reference on reference_blockNumber
df = df_horizons.merge(df_reference, how='left', on=reference_block_label)
df.to_csv(os.path.join(processed_results_dir, "df.csv"))

null_counts = df.isnull().sum()



# Showcase tables
pass










showcase_cols = ['blockNumber', 'min_flag', 'reference_blockNumber', 'horizon_label', 'cum_volume_500']
df_horizons.iloc[108757:109052][showcase_cols]

#df.iloc[108757:109052][['blockNumber', 'reference_blockNumber_500', 'reference_blockNumber_3000', 'horizon', 'blsame_1', 'cum_volume_500']]
#15552772


# NOTE -> We have duplicated reference_blockNumber values. This is because we have multiple pools that have the same reference_blockNumber.
# But since we are filtering df_blocks_full to only include the reference pool, we should not have this duplication.
# Remove null values from analysis
df_notnull = df_reference[df_reference[reference_block_label].notnull()]
dups = df_notnull[df_notnull[reference_block_label].duplicated()][reference_block_label]
assert len(dups) == 0

# # For analysis on full blocks (in the future, and if needed):
# dup_example = df_notnull[df_notnull[reference_block_label].duplicated()].iloc[0][reference_block_label]
# df_reference[df_reference['reference_blockNumber'] == dup_example]
# df[df['reference_blockNumber'] == dup_example]


#We now proceed to a detailed investigation of our forecasting ability regarding the incoming trading volume on pool 3000, after a mint operation occurred on pool 500
target_variable = 'cum_volume_3000_ref500'
explainable_variables = df_direct_pool.columns.tolist() # For now, all columns. Later, we will do some aggregations, etc.

# We test forecasting horizons every ten blocks up to the next mint operation on either pool
#start with p distinct predictors and assume that there is approximately a linear relationship between the predictors and the response Y.
#The linear model takes the form
#Y ≈ β0 + β1X1 + β2X2 + · · · + βpXp

#we compute estimates βˆ0, βˆ1, ..., βˆp of the coefficients of the model via the least-squares approach
#Y ≈ βˆ0 + βˆ1X1 + βˆ2X2 + · · · + βˆpXp

import statsmodels.api as sm

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

# Fit the linear regression model
model = sm.OLS(y, X)
results = model.fit()

# Print the comprehensive summary output
print(results.summary())


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
plt.suptitle(f'Mint of reference on Pool: {reference_pool_mint}')
plt.show()


pass