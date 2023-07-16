
import pandas as  pd
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor

from utils.visualisations import print_heatmap, print_highest_corr
from utils.clean_independent import replace_nulls, remove_nulls, aggregate_columns_by_interval

# Variable families
from utils.cols_management import explainable_variables, cols_drop_correlated, cols_aggregate_intervals_range


reference_pool_mints = [500, 3000]
reference_pool = 500
horizon = 15

target_variable = 'cum_volume_3000'

def calculate_vif(X):
    vif = pd.DataFrame()
    vif["Variable"] = X.columns
    vif["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    vif.sort_values(by='VIF', ascending=False, inplace=True)

    thresholds = [1, 5, 10]
    counts = {}
    for threshold in thresholds:
        counts[threshold] = len(vif[vif['VIF'] > threshold])
    print("VIF counts for thresholds: ", thresholds)
    print(counts)
    return vif

# Read df_features_raw
df_all = pd.read_csv(f"Data/processed/df_features_raw_ref{reference_pool}.csv")
df = df_all[df_all['horizon_label'] == horizon]

df = replace_nulls(df)

X = df[explainable_variables]
y = df[target_variable]

# Further feature engineer
X_dropped = X.drop(columns=cols_drop_correlated)
X_aggregated, _ = aggregate_columns_by_interval(X_dropped, cols_aggregate_intervals_range)

# Supporting analysis
print_highest_corr(X.corr())
print_highest_corr(X_dropped.corr())

print_heatmap(X.corr(), "Correlation Matrix of Independent Variables - Before Multicollinearity")
print_heatmap(X_aggregated.corr(), "Correlation Matrix of Independent Variables - After Multicollinearity")

# identify multicollinearity
vif_raw = calculate_vif(X)
vif = calculate_vif(X_aggregated)

#Notes:
## binance-count and binance-btc highly correlated




# # Fit the OLS model
# model = sm.OLS(y, X)
# results = model.fit()

# # Retrieve the R-squared value
# r_squared = results.rsquared



# # remove cols with multicollinearity
# X_red = X.drop(columns = vif[vif['VIF'] > 10]['Variable'])





# corr_matrix_simpler = X_red.corr()
# print_heatmap(corr_matrix_simpler, title_after)


