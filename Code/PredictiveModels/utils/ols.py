
import statsmodels.api as sm
import pandas as pd
import matplotlib.pyplot as plt

def run_for_all_horizons(df, target_variable, explainable_variables):

    # Limit the data to the first 30 horizons (i.e., 300 blocks) The average block time for Ethereum is approximately 13-15 seconds. 300*15=4500 seconds = 75 minutes
    horizon_labels = df['horizon_label'].unique() 

    r_squared_values = []
    r_squared_adj_values = []
    horizon_values = []
    observation_counts = []


    for horizon in horizon_labels:
        subset_data = df[df['horizon_label'] == horizon]
        X = subset_data[explainable_variables]
        y = subset_data[target_variable]

        # Fit the OLS model
        model = sm.OLS(y, X)
        results = model.fit()

        # Retrieve the R-squared value
        r_squared = results.rsquared
        r_squared_adj = results.rsquared_adj
        
        r_squared_values.append(r_squared)
        r_squared_adj_values.append(r_squared_adj)
        horizon_values.append(horizon * 10)
        observation_counts.append(len(X))


    return r_squared_values, r_squared_adj_values, horizon_values, observation_counts

def stepwise_selection(df, target, explanatory_vars, significance_level=0.05):
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