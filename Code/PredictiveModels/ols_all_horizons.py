import pandas as pd
from utils.visualisations import display_figures, plot_dataframes
from utils.clean_independent import replace_nulls, remove_nulls, aggregate_columns_by_interval
from utils.cols_management import explainable_variables, cols_drop_correlated, cols_aggregate_intervals_range, cols_replace_nulls
import utils.ols as ols
import matplotlib.pyplot as plt

def filter_explainable_variables(variables, remove_list):
    """Remove specified variables from the list of explainable variables."""
    return [var for var in variables if var not in remove_list]


def prepare_dataframe(df_raw, remove_list):
    """Prepare dataframe by cleaning and removing unnecessary columns."""
    df = replace_nulls(df_raw, cols_replace_nulls)
    df_filtered = df[df['horizon_label'] <= 30]
    explainable_variables_filtered = filter_explainable_variables(explainable_variables, remove_list)
    df_dropped = df_filtered.drop(remove_list, axis=1)
    return remove_nulls(df_dropped, explainable_variables_filtered, logs=True), explainable_variables_filtered

def prepare_dataframe_engineered(df_raw, remove_list, aggregate_list):
    """Prepare dataframe with further feature engineering."""
    df_dropped = df_raw.drop(columns=remove_list)
    explainable_variables_filtered = filter_explainable_variables(explainable_variables, remove_list)
    df_na = remove_nulls(df_dropped, explainable_variables_filtered, logs=True)
    df_aggregated, dropped = aggregate_columns_by_interval(df_na, aggregate_list)
    explainable_variables_filtered_aggregated = filter_explainable_variables(explainable_variables_filtered, dropped)
    return df_aggregated, explainable_variables_filtered_aggregated


def run_and_store_ols_results(df, target_variable, explainable_variables, return_args_dict, key):
    """Run OLS for all horizons and store results in the dictionary."""
    return_args_dict[key] = ols.run_for_all_horizons(df, target_variable, explainable_variables)


def create_figures_dict(r_squared_values, horizon_values, observation_counts, reference_pool, title):
    """Create dictionary of figure data."""
    return {
        "r_squared_values": r_squared_values, 
        "horizon_values": horizon_values, 
        "observation_counts": observation_counts, 
        "reference_pool": title + str(reference_pool)
    }


target_variables = ['cum_volume_500', 'cum_volume_3000', 'cum_volume_base']

run_args = {
    3000: target_variables,
    500: target_variables
}
return_args = {}
all_figs = []

remove_list = ['vol_0_1', 'vol_0_2', 'vol_0_3', 'avg-USD-iother_01', 'rate-USD-iother_01']

for reference_pool, target_variables in run_args.items():
    return_args[reference_pool] = {}

    for target_variable in target_variables:
        return_args[reference_pool][target_variable] = {}

        df_raw = pd.read_csv(f"Data/processed/features/df_features_raw_ref{reference_pool}.csv")
        df, explainable_variables_filtered = prepare_dataframe(df_raw, remove_list)
        run_and_store_ols_results(df, target_variable, explainable_variables_filtered, return_args[reference_pool][target_variable], 'all_features')

        # Further feature engineering
        df_raw2 = pd.read_csv(f"Data/processed/features/df_features_raw_ref{reference_pool}.csv")
        df2, _ = prepare_dataframe(df_raw2, [])
        df_engineered, explainable_variables_filtered_aggregated = prepare_dataframe_engineered(df2, cols_drop_correlated, cols_aggregate_intervals_range)
        run_and_store_ols_results(df_engineered, target_variable, explainable_variables_filtered_aggregated, return_args[reference_pool][target_variable], 'reduced_multicollinearity')

        # Step-wise feature selection
        df_raw3 = pd.read_csv(f"Data/processed/features/df_features_raw_ref{reference_pool}.csv")
        df3, _ = prepare_dataframe(df_raw3, [])
        df_engineered, explainable_variables_filtered_aggregated = prepare_dataframe_engineered(df3, cols_drop_correlated, cols_aggregate_intervals_range)
        selected_features = ols.stepwise_selection(df, df[target_variable], explainable_variables_filtered)

        pass

base_df = pd.read_csv(f"Data/processed/features/df_features_raw_refbase.csv")
base_df = base_df[base_df['horizon_label'] <= 30]
base_df['blocks'] = base_df['horizon_label'] * 10
df_pool_counter = base_df.groupby(['pool', 'blocks'])['reference_blockNumber'].count().reset_index()
df_pool_counter_pivot = df_pool_counter.pivot(index='blocks', columns='pool', values='reference_blockNumber')

# Replace `run_args` with your actual dictionary
plot_dataframes(return_args, df_pool_counter_pivot)

pass