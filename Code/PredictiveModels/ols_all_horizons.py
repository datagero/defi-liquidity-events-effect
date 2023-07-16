
import pandas as  pd
import matplotlib.pyplot as plt

from utils.visualisations import print_heatmap, print_highest_corr, load_multiple_r2_figs, display_figures
from utils.clean_independent import replace_nulls, remove_nulls, aggregate_columns_by_interval

# Variable families
from utils.cols_management import explainable_variables, cols_drop_correlated, cols_aggregate_intervals_range

import utils.ols as ols


run_args = {
    3000: 'cum_volume_500',
    500: 'cum_volume_3000'
}

return_args = {}
all_figs = []

for reference_pool, target_variable in run_args.items():
    return_args[reference_pool] = {}

    # Read df_features_raw
    df = pd.read_csv(f"Data/processed/df_features_raw_ref{reference_pool}.csv")
    df = replace_nulls(df)

    df_filtered = df[df['horizon_label'] <= 30]

    # Remove the columns that are not needed or pending further cleaning
    remove = ['vol_0_1', 'vol_0_2', 'vol_0_3', 'avg-USD-iother_01', 'rate-USD-iother_01']
    explainable_variables_filtered = [x for x in explainable_variables if x not in remove]
    df_dropped = df_filtered.drop(remove, axis=1)
    df_na = remove_nulls(df_dropped, explainable_variables_filtered, logs=True)

    return_args[reference_pool]['all_features'] = ols.run_for_all_horizons(df_na, target_variable, explainable_variables_filtered)

    # Further feature engineer
    df2_dropped = df_filtered.drop(columns=cols_drop_correlated)
    explainable_variables_filtered2 = [x for x in explainable_variables if x not in cols_drop_correlated]
    df2_na = remove_nulls(df2_dropped, explainable_variables_filtered2, logs=True)
    df2_aggregated, dropped = aggregate_columns_by_interval(df2_na, cols_aggregate_intervals_range)
    explainable_variables_filtered2_2 = [x for x in explainable_variables_filtered2 if x not in dropped]

    return_args[reference_pool]['reduced_multicollinearity'] = ols.run_for_all_horizons(df2_aggregated, target_variable, explainable_variables_filtered2_2)


    r_squared_values, r_squared_adj_values, horizon_values, observation_counts = return_args[reference_pool]['all_features']
    r_squared_values2, r_squared_adj_values2, horizon_values2, observation_counts2 = return_args[reference_pool]['reduced_multicollinearity']

    fig1 = {
        "r_squared_values": r_squared_values, 
        "horizon_values": horizon_values, 
        "observation_counts": observation_counts, 
        "reference_pool": "All Features - " + str(reference_pool)
    }

    fig2 = {
        "r_squared_values": r_squared_values2, 
        "horizon_values": horizon_values2, 
        "observation_counts": observation_counts2, 
        "reference_pool": "Reduced Multicollinearity - " + str(reference_pool)
    }

    all_figs.extend([fig1, fig2])

display_figures(all_figs)



    # fig1 = load_multiple_r2_figs(r_squared_values, horizon_values, observation_counts, "All Features - " + str(reference_pool))
    # fig2 = load_multiple_r2_figs(r_squared_values2, horizon_values2, observation_counts2, "Reduced Multicollinearity - " + str(reference_pool))

    # all_figs.append(fig1)
    # all_figs.append(fig2)


    # # Display all the figures together
    # for fig in all_figs:
    #     plt.show()


# Split the data into target variable and explanatory variables




# best horizon for marginal gain r-squared

# Balance R-squared and Observation Counts: Find a balance between R-squared and observation counts. 
# A higher R-squared with a reasonable number of observations generally indicates a stronger model. 
# However, a high R-squared with a very low observation count may indicate overfitting, 
# where the model is fitting the noise or idiosyncrasies of the limited data rather than capturing meaningful patterns.

# def select_model(r_squared_values, observation_counts):
#     if len(r_squared_values) != len(observation_counts):
#         raise ValueError("Lengths of input lists must match.")

#     best_ratio = r_squared_values[1] / observation_counts[1]  # Initialize with the second model
#     best_model_index = 1

#     for i in range(2, len(r_squared_values)):  # Start from the second model
#         ratio = r_squared_values[i] / observation_counts[i]
#         if ratio > best_ratio:
#             best_ratio = ratio
#             best_model_index = i

#     best_r_squared = r_squared_values[best_model_index]
#     best_observation_count = observation_counts[best_model_index]

#     return best_model_index, best_r_squared, best_observation_count




pass