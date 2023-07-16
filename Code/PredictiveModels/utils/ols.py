
import statsmodels.api as sm
import matplotlib.pyplot as plt

def run_for_all_horizons(df, target_variable, explainable_variables, reference_pool='Not Defined'):

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
