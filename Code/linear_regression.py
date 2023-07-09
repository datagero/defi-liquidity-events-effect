
import os
import pandas as pd
import matplotlib.pyplot as plt
from feature_engineering.build_intervals import calculate_horizons

# Define the directory path where the files are saved
interim_results_dir = "Data/interim_results"
processed_results_dir = "Data/processed"

# Open the CSV files as DataFrames
df_reduced = pd.read_csv(os.path.join(interim_results_dir, "df_reduced.csv"))
df_blocks_full = pd.read_csv(os.path.join(interim_results_dir, "df_blocks_full.csv"))
df_direct_pool = pd.read_csv(os.path.join(processed_results_dir, "direct_pool.csv"), index_col=0)

df_horizons = calculate_horizons(df_reduced, step=10)

# join df_direct_pool with df_blocks_full
df_blocks = df_blocks_full[['hashid', 'blockNumber']]
df_blocks = df_blocks.rename(columns={'blockNumber': 'reference_blockNumber_500'})
df_reference = df_direct_pool.merge(df_blocks, how='left', left_index=True, right_on='hashid')

# join df with df_reference on reference_blockNumber
df = df_horizons.merge(df_reference, how='left', on='reference_blockNumber_500')
null_counts = df.isnull().sum()


#We now proceed to a detailed investigation of our forecasting ability regarding the incoming trading volume on pool 500, after a mint operation occurred on pool 3000
target_variable = 'cum_volume_3000'
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
    print("===========", horizon, "===========")
    print("Current row count:", len(X))
    remove = ['vol_0_1', 'vol_0_2', 'vol_0_3', 'avg-USD-iother_01', 'rate-USD-iother_01']
    X = X.drop(remove, axis=1)

    # Remove the rows that contain null values
    null_rows = X[X.isnull().any(axis=1)].index
    X = X.drop(null_rows, axis=0)
    y = y.drop(null_rows, axis=0)
    print("New row count:", len(X))

    # Fit the OLS model
    model = sm.OLS(y, X)
    results = model.fit()

    # Retrieve the R-squared value
    r_squared = results.rsquared
    
    r_squared_values.append(r_squared)
    horizon_values.append(horizon * 10)
    observation_counts.append(len(X))

fig, ax1 = plt.subplots()
ax2 = ax1.twinx()

ax1.scatter(horizon_values, r_squared_values, color='b')
ax1.set_xlabel('Horizon')
ax1.set_ylabel('R-squared', color='b')
ax1.set_ylim(0, 1)  # Set the y-axis range for R-squared

ax2.plot(horizon_values, observation_counts, color='r')
ax2.set_ylabel('Observation Count', color='r')
ax2.set_ylim(0, max(observation_counts) + 1000)  # Set the y-axis range for observation count

plt.title('R-squared and Observation Count vs. Horizon')
plt.show()

pass