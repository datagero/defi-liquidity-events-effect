import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor
import math

from utils.visualisations import print_heatmap_list, print_highest_corr, dict_to_dataframe, advanced_plot_dataframes, residuals_individual, residuals_horizons
from utils.clean_independent import replace_nulls, remove_nulls, aggregate_columns_by_interval

# Variable families
from utils.cols_management import explainable_variables, cols_drop_correlated, cols_aggregate_intervals_range, cols_replace_nulls
import utils.ols as ols


"""
This script conducts Ordinary Least Squares (OLS) regression analyses in three primary steps: initial analysis, individual run OLS, and all horizons run OLS.
The analyses are carried out on datasets from a specific pool of a decentralized exchange to predict cumulative volume. The script consists of the following steps:

first_analysis function: This function starts by loading the data, filtering it based on a single horizon label, replacing null values, and removing rows with nulls in certain fields.
It then divides the dataset into dependent and explanatory variables. From here, the function drops highly correlated columns from the dataset and aggregates remaining columns by interval.
It provides a visual representation of the correlations with a heatmap, and calculates Variance Inflation Factors (VIF) to identify multicollinearity among the variables.

individual_run_ols function: This function is similar to first_analysis but goes a step further.
It uses a stepwise selection method to select the best features for the model, and then runs an OLS regression for a single specified horizon.
This function also provides a visual representation of the residuals from the model, assisting in understanding the model's performance for the specific horizon.

all_horizons_run_ols function: The final function performs a similar set of steps as individual_run_ols
but applies the OLS regression model across all horizons in the data,rather than a single horizon.
This allows it to provide a comprehensive analysis of the model's performance across various prediction horizons.
It generates plots of residuals for each horizon, facilitating a clear understanding and comparison of model performance across different horizons.

Throughout these steps, the script uses helper functions from a utility module to perform tasks such as replacing nulls, aggregating columns, stepwise selection, and conducting OLS regression.
"""


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

    thresholds = [0, 1, 5, math.inf]
    counts = {threshold: len(vif[(vif['VIF'] >= thresholds[thresholds.index(threshold)-1]) & (vif['VIF'] < threshold)]) for threshold in thresholds[1:]}
    print("VIF counts for thresholds:", thresholds[1:])
    print(counts)

    return vif


def first_analysis():
    # Defining constants
    REFERENCE_POOL = 3000
    HORIZON = 15
    TARGET_VARIABLE = 'cum_volume_500'

    # Load data and filter based on the horizon label
    df_all = pd.read_csv(f"Data/processed/features/df_features_raw_ref{REFERENCE_POOL}.csv")
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


    print_heatmap_list([X.corr(), X_aggregated.corr()], 
                    ["Before Multicollinearity Reduction", "After Multicollinearity Reduction and Feature Aggregation"],
                    output_path="Other Resources/correlation_matrix.png")


    # Calculate and display VIF
    vif_raw = calculate_vif(X)
    vif = calculate_vif(X_aggregated)
    pass

def individual_run_ols():
    # Defining constants
    REFERENCE_POOL = 3000
    HORIZON = 20
    target_variable = 'cum_volume_500'

    # Load data and filter based on the horizon label
    df_all = pd.read_csv(f"Data/processed/features/df_features_raw_ref{REFERENCE_POOL}.csv")
    df = df_all[df_all['horizon_label'] == HORIZON]

    # Replace nulls in the DataFrame
    df = replace_nulls(df, cols_replace_nulls)
    df = remove_nulls(df, explainable_variables, logs=True)

    df2, _ = ols.prepare_dataframe(df, [])
    df_engineered2, explainable_variables_filtered_aggregated2 = ols.prepare_dataframe_engineered(df2, cols_drop_correlated, cols_aggregate_intervals_range)
    selected_features = ols.stepwise_selection(df_engineered2, df_engineered2[target_variable], explainable_variables_filtered_aggregated2)
    # Print features dropped by step-wise
    print(f"Features dropped by step-wise for {target_variable}: {set(explainable_variables_filtered_aggregated2) - set(selected_features)}")

    for log_target in [True]:
        models, train_metrics, test_metrics, test_data, test_predictions = ols.cross_validate_for_all_horizons(df_engineered2, target_variable, selected_features, k=5, log_target=log_target)
        residuals_individual(models[list(models.keys())[-1]], test_data[-1], test_predictions[-1], log_target=log_target,
                             title=f"Pool: {REFERENCE_POOL} - OLS Residuals for Horizon {HORIZON} (Log Target: {log_target})")

    pass

def all_horizons_run_ols():
    # Defining constants
    REFERENCE_POOL = 3000
    target_variable = 'cum_volume_500'

    # Load data and filter based on the horizon label
    df_all = pd.read_csv(f"Data/processed/features/df_features_raw_ref{REFERENCE_POOL}.csv")
    df = df_all[df_all['horizon_label'] <= 30]

    # Replace nulls in the DataFrame
    df = replace_nulls(df, cols_replace_nulls)
    df = remove_nulls(df, explainable_variables, logs=True)

    df2, _ = ols.prepare_dataframe(df, [])
    df_engineered2, explainable_variables_filtered_aggregated2 = ols.prepare_dataframe_engineered(df2, cols_drop_correlated, cols_aggregate_intervals_range)
    selected_features = ols.stepwise_selection(df_engineered2, df_engineered2[target_variable], explainable_variables_filtered_aggregated2)
    # Print features dropped by step-wise
    print(f"Features dropped by step-wise for {target_variable}: {set(explainable_variables_filtered_aggregated2) - set(selected_features)}")

    for log_target in [True]:
        models, train_metrics, test_metrics, test_data, test_predictions = ols.cross_validate_for_all_horizons(df_engineered2, target_variable, selected_features, k=5, log_target=log_target)

        # Residuals
        horizons = df_engineered2['horizon_label'].unique() * 10
        residuals_horizons(test_data, test_predictions, horizons, title=f"Pool {REFERENCE_POOL} - Residuals for all Horizons")


    pass
# first_analysis()
individual_run_ols()
all_horizons_run_ols()


