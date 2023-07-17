import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor

from utils.visualisations import print_heatmap, print_highest_corr
from utils.clean_independent import replace_nulls, remove_nulls, aggregate_columns_by_interval

# Variable families
from utils.cols_management import explainable_variables, cols_drop_correlated, cols_aggregate_intervals_range, cols_replace_nulls

# Defining constants
REFERENCE_POOL = 3000
HORIZON = 15
TARGET_VARIABLE = 'cum_volume_500'


def calculate_vif(X):
    """
    Calculate Variance Inflation Factor (VIF).

    Parameters
    ----------
    X : DataFrame
        DataFrame with independent variables.

    Returns
    -------
    DataFrame
        DataFrame with variables and corresponding VIF.
    """
    vif = pd.DataFrame()
    vif["Variable"] = X.columns
    vif["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    vif.sort_values(by='VIF', ascending=False, inplace=True)

    thresholds = [1, 5, 10]
    counts = {threshold: len(vif[vif['VIF'] > threshold]) for threshold in thresholds}
    print("VIF counts for thresholds: ", thresholds)
    print(counts)

    return vif


# Load data and filter based on the horizon label
df_all = pd.read_csv(f"Data/processed/df_features_raw_ref{REFERENCE_POOL}.csv")
df = df_all[df_all['horizon_label'] == HORIZON]

# Replace nulls in the DataFrame
df = replace_nulls(df, cols_replace_nulls)
df = remove_nulls(df, explainable_variables, logs=True)

# Extract independent and dependent variables
X = df[explainable_variables]
y = df[TARGET_VARIABLE]

# Perform feature engineering by dropping correlated columns and aggregating by interval
X_dropped = X.drop(columns=cols_drop_correlated)
X_aggregated, _ = aggregate_columns_by_interval(X_dropped, cols_aggregate_intervals_range)

# Display correlation information
print_highest_corr(X.corr())
print_highest_corr(X_dropped.corr())

print_heatmap(X.corr(), "Correlation Matrix of Independent Variables - Before Multicollinearity")
print_heatmap(X_aggregated.corr(), "Correlation Matrix of Independent Variables - After Multicollinearity")

# Calculate and display VIF
vif_raw = calculate_vif(X)
vif = calculate_vif(X_aggregated)
