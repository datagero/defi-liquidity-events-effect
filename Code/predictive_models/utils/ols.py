
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
from utils.clean_independent import replace_nulls, remove_nulls, aggregate_columns_by_interval
from utils.cols_management import explainable_variables, cols_replace_nulls


import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.model_selection import GroupKFold
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from scipy.stats import t


def filter_explainable_variables(variables, remove_list, new_list=[]):
    """Remove specified variables from the list of explainable variables."""
    inscope = [var for var in variables if var not in remove_list]
    to_add = [var for var in new_list if var not in inscope]
    return inscope + to_add

def prepare_dataframe(df_raw, remove_list):
    """Prepare dataframe by cleaning and removing unnecessary columns."""
    # Limit the data to the first 30 horizons (i.e., 300 blocks) The average block time for Ethereum is approximately 13-15 seconds. 300*15=4500 seconds = 75 minutes
    df = replace_nulls(df_raw, cols_replace_nulls)
    df_filtered = df[df['horizon_label'] <= 30]
    explainable_variables_filtered = filter_explainable_variables(explainable_variables, remove_list)
    df_dropped = df_filtered.drop(remove_list, axis=1)
    return remove_nulls(df_dropped, explainable_variables_filtered, logs=True), explainable_variables_filtered

def prepare_dataframe_engineered(df_raw, remove_list, aggregate_list):
    """Prepare dataframe with further feature engineering."""
    # Limit the data to the first 30 horizons (i.e., 300 blocks) The average block time for Ethereum is approximately 13-15 seconds. 300*15=4500 seconds = 75 minutes
    df_dropped = df_raw.drop(columns=remove_list)
    df_filtered = df_dropped[df_dropped['horizon_label'] <= 30]
    explainable_variables_filtered = filter_explainable_variables(explainable_variables, remove_list)
    df_na = remove_nulls(df_filtered, explainable_variables_filtered, logs=True)
    df_aggregated, dropped, new = aggregate_columns_by_interval(df_na, aggregate_list)
    explainable_variables_filtered_aggregated = filter_explainable_variables(explainable_variables_filtered, dropped, new)
    return df_aggregated, explainable_variables_filtered_aggregated


def create_figures_dict(r_squared_values, horizon_values, observation_counts, reference_pool, title):
    """Create dictionary of figure data."""
    return {
        "r_squared_values": r_squared_values, 
        "horizon_values": horizon_values, 
        "observation_counts": observation_counts, 
        "reference_pool": title + str(reference_pool)
    }

def split_data(df, test_size=0.2):
    unique_block_numbers = df['reference_blockNumber'].unique()
    train_block_numbers, test_block_numbers = train_test_split(unique_block_numbers, test_size=test_size, random_state=42)

    df_train = df[df['reference_blockNumber'].isin(train_block_numbers)]
    df_test = df[df['reference_blockNumber'].isin(test_block_numbers)]
    
    return df_train, df_test

def train_and_predict_for_all_horizons(df, target_variable, explainable_variables):

    horizon_labels = df['horizon_label'].unique()
    df_train, df_test = split_data(df)  # Split the data

    trained_models = {}
    train_metrics = []
    test_metrics = []

    for horizon in horizon_labels:
        # Train
        train_subset = df_train[df_train['horizon_label'] == horizon]
        X_train = train_subset[explainable_variables].values
        y_train = train_subset[target_variable].values

        # Fit the OLS model
        model = sm.OLS(y_train, X_train)
        results = model.fit()

        trained_models[horizon] = results

        # Retrieve the R-squared value
        r_squared_train = results.rsquared
        r_squared_adj_train = results.rsquared_adj

        train_metrics.append((horizon * 10, len(X_train), r_squared_train, r_squared_adj_train))

        # Test
        test_subset = df_test[df_test['horizon_label'] == horizon]
        X_test = test_subset[explainable_variables].values
        y_test = test_subset[target_variable].values

        # Predict the test data
        y_pred = results.predict(X_test)

        # Total Sum of Squares (TSS)
        y_mean = np.mean(y_test)
        TSS = np.sum((y_test - y_mean)**2)

        # Residual Sum of Squares (RSS)
        residuals = y_test - y_pred
        RSS = np.sum(residuals**2)

        # R-squared
        r_squared_test = 1 - (RSS / TSS)

        # Adjusted R-squared
        n = len(y_test)  # number of observations
        p = len(explainable_variables)  # number of predictors
        r_squared_adj_test = 1 - (1 - r_squared_test) * ((n - 1) / (n - p - 1))

        test_metrics.append((horizon * 10, len(X_test), r_squared_test, r_squared_adj_test))

    return trained_models, train_metrics, test_metrics

def cross_validate_for_all_horizons(df, target_variable, explainable_variables, k=5, log_target=True):
    horizon_labels = df['horizon_label'].unique()

    trained_models = {}
    train_metrics = []  # List to store train metrics
    test_metrics = []  # List to store test metrics

    test_data = []  # List to store test data
    test_predictions = []

    gkf = GroupKFold(n_splits=k)

    for horizon in horizon_labels:
        subset = df[df['horizon_label'] == horizon]
        X = subset[explainable_variables].values
        y = subset[target_variable].values

        groups = subset['reference_blockNumber']  # Groups for GroupKFold

        train_r_squared_values = []
        train_r_squared_adj_values = []
        test_r_squared_values = []
        test_r_squared_adj_values = []

        for train_index, test_index in gkf.split(X, y, groups):
            X_train, X_test = X[train_index], X[test_index]
            y_train, y_test = y[train_index], y[test_index]

            #compute the standard deviation of the features in the train set, and we divide both the train and test sets by these values
            X_train_std = np.std(X_train, axis=0)
            X_train = X_train / X_train_std
            X_test = X_test / X_train_std

            # Replace zeros with very small values
            y_train[y_train == 0] = 1e-10
            y_test[y_test == 0] = 1e-10
            
            if log_target:
                # Taking the logarithm in base 10 of our responses
                y_train = np.log10(y_train)
                y_test = np.log10(y_test)

            X_train = sm.add_constant(X_train)
            X_test = sm.add_constant(X_test)

            # Fit the OLS model
            model = sm.OLS(y_train, X_train)
            results = model.fit()

            trained_models[horizon] = results

            # Predict the train data
            y_train_pred = results.predict(X_train)
            # Predict the test data
            y_test_pred = results.predict(X_test)

            # Total Sum of Squares (TSS)
            y_train_mean = np.mean(y_train)
            TSS_train = np.sum((y_train - y_train_mean)**2)
            y_test_mean = np.mean(y_test)
            TSS_test = np.sum((y_test - y_test_mean)**2)

            # Residual Sum of Squares (RSS)
            train_residuals = y_train - y_train_pred
            RSS_train = np.sum(train_residuals**2)
            test_residuals = y_test - y_test_pred
            RSS_test = np.sum(test_residuals**2)

            # R-squared
            train_r_squared = 1 - (RSS_train / TSS_train)
            test_r_squared = 1 - (RSS_test / TSS_test)

            # Adjusted R-squared
            n_train = len(y_train)  # number of train observations
            p = len(explainable_variables)  # number of predictors
            train_r_squared_adj = 1 - (1 - train_r_squared) * ((n_train - 1) / (n_train - p - 1))
            n_test = len(y_test)  # number of test observations
            test_r_squared_adj = 1 - (1 - test_r_squared) * ((n_test - 1) / (n_test - p - 1))

            train_r_squared_values.append(train_r_squared)
            train_r_squared_adj_values.append(train_r_squared_adj)
            test_r_squared_values.append(test_r_squared)
            test_r_squared_adj_values.append(test_r_squared_adj)

            # Store the test data and predictions from the last fold
            test_data.append((X_test, y_test))
            test_predictions.append(y_test_pred)

        train_metrics.append((horizon * 10, len(X), np.mean(train_r_squared_values), np.mean(train_r_squared_adj_values)))
        test_metrics.append((horizon * 10, len(X), np.mean(test_r_squared_values), np.mean(test_r_squared_adj_values)))

    return trained_models, train_metrics, test_metrics, test_data, test_predictions



def stepwise_selection(df, target, explanatory_vars, significance_level=0.10):
    initial_features = explanatory_vars.copy()
    best_features = []

    while len(initial_features) > 0:
        remaining_features = list(set(initial_features) - set(best_features))
        new_pval = pd.Series(index=remaining_features, dtype='float64')
        for new_column in remaining_features:
            model = sm.OLS(target, sm.add_constant(df[best_features+[new_column]])).fit()
            new_pval[new_column] = model.pvalues[new_column]
        min_p_value = new_pval.min()
        if min_p_value < significance_level:
            best_features.append(new_pval.idxmin())
        else:
            break

    # Backward elimination
    while len(best_features) > 0:
        best_features_with_constant = sm.add_constant(df[best_features])
        p_values = sm.OLS(target, best_features_with_constant).fit().pvalues[1:]  # Exclude intercept
        max_p_value = p_values.max()
        if max_p_value >= significance_level:
            excluded_feature = p_values.idxmax()
            best_features.remove(excluded_feature)
        else:
            break 

    return best_features

def compute_evaluation_metrics(y_true, y_pred):
    mae = mean_absolute_error(y_true, y_pred)
    mse = mean_squared_error(y_true, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_true, y_pred)
    return mae, mse, rmse, r2

def residual_analysis(y_true, y_pred):
    residuals = y_true - y_pred
    plt.scatter(y_pred, residuals)
    plt.xlabel('Predicted Values')
    plt.ylabel('Residuals')
    plt.title('Residual Plot')
    plt.show()

def ols_pvalues(X, y, alpha=0.05):
    model = sm.OLS(y, sm.add_constant(X)).fit()
    confidence_intervals = model.conf_int(alpha)
    p_values = model.pvalues
    for variable, p_value in p_values.iteritems():
        if p_value < alpha:
            print(f"{variable} is statistically significant.")
        else:
            print(f"{variable} is not statistically significant.")
    return confidence_intervals, p_values

def apply_model_and_evaluate(model, X_train, X_test, y_train, y_test):
    # Fit the model on the training data
    model.fit(X_train, y_train)
    
    # Make predictions on the training set
    y_train_pred = model.predict(X_train)

    # Compute metrics on the training set
    mae_train, mse_train, rmse_train, r2_train = compute_evaluation_metrics(y_train, y_train_pred)

    # Make predictions on the test set
    y_test_pred = model.predict(X_test)

    # Compute metrics on the test set
    mae_test, mse_test, rmse_test, r2_test = compute_evaluation_metrics(y_test, y_test_pred)

    # Residual analysis on the test set
    residual_analysis(y_test, y_test_pred)

    # Compute p-values on the training set
    confidence_intervals, p_values = ols_pvalues(X_train, y_train)

    return {
        'train_metrics': {'mae': mae_train, 'mse': mse_train, 'rmse': rmse_train, 'r2': r2_train},
        'test_metrics': {'mae': mae_test, 'mse': mse_test, 'rmse': rmse_test, 'r2': r2_test},
        'confidence_intervals': confidence_intervals,
        'p_values': p_values
    }
