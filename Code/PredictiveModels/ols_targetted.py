import pandas as pd
from sklearn.model_selection import train_test_split
from utils.visualisations import plot_dataframes
from utils.cols_management import cols_drop_correlated, cols_aggregate_intervals_range
import utils.ols as ols


target_variables = ['cum_volume_500', 'cum_volume_3000', 'cum_volume_base']

run_args = {
    3000: target_variables,
    500: target_variables
}
return_args = {}
all_figs = []

remove_list = ['vol_0_1', 'vol_0_2', 'vol_0_3', 'avg-USD-iother_01', 'rate-USD-iother_01']

def run_and_store_ols_results(df, target_variable, explainable_variables):
    """Run OLS for all horizons and store results in the dictionary."""
    return ols.run_for_all_horizons(df, target_variable, explainable_variables)

def split_data(df, test_size=0.2):
    unique_block_numbers = df['reference_blockNumber'].unique()
    train_block_numbers, test_block_numbers = train_test_split(unique_block_numbers, test_size=test_size, random_state=42)

    df_train = df[df['reference_blockNumber'].isin(train_block_numbers)]
    df_test = df[df['reference_blockNumber'].isin(test_block_numbers)]
    
    return df_train, df_test

for reference_pool, target_variables in run_args.items():
    return_args[reference_pool] = {}
    # return_args[reference_pool]['all_features'] = {'train': {}, 'test_data': {}}
    # return_args[reference_pool]['reduced_multicollinearity'] = {'train': {}, 'test_data': {}}
    return_args[reference_pool]['step-wise'] = {'train': {}, 'test_data': {}}
    for target_variable in target_variables:

        if 'all_features' in return_args[reference_pool]:
            df_raw = pd.read_csv(f"Data/processed/features/df_features_raw_ref{reference_pool}.csv")
            df, explainable_variables_filtered = ols.prepare_dataframe(df_raw, remove_list)
            df_train, df_test = split_data(df)  # Split the data
            return_args[reference_pool]['all_features']['train'][target_variable] = run_and_store_ols_results(df_train, target_variable, explainable_variables_filtered)
            return_args[reference_pool]['all_features']['test_data'][target_variable] = df_test  # Save test data
        if 'reduced_multicollinearity' in return_args[reference_pool]:
            # Further feature engineering
            df_raw2 = pd.read_csv(f"Data/processed/features/df_features_raw_ref{reference_pool}.csv")
            df2, _ = ols.prepare_dataframe(df_raw2, [])
            df_engineered, explainable_variables_filtered_aggregated = ols.prepare_dataframe_engineered(df2, cols_drop_correlated, cols_aggregate_intervals_range)
            df_engineered_train, df_engineered_test = split_data(df_engineered)  # Split the engineered data
            return_args[reference_pool]['reduced_multicollinearity']['train'][target_variable] = run_and_store_ols_results(df_engineered_train, target_variable, explainable_variables_filtered_aggregated)
            return_args[reference_pool]['reduced_multicollinearity']['test_data'][target_variable] = df_engineered_test  # Save engineered test data
        if 'step-wise' in return_args[reference_pool]:
            # Step-wise feature selection
            df_raw3 = pd.read_csv(f"Data/processed/features/df_features_raw_ref{reference_pool}.csv")
            df3, _ = ols.prepare_dataframe(df_raw3, [])
            df_engineered2, explainable_variables_filtered_aggregated2 = ols.prepare_dataframe_engineered(df3, cols_drop_correlated, cols_aggregate_intervals_range)
            selected_features = ols.stepwise_selection(df3, df3[target_variable], explainable_variables_filtered_aggregated2)
            # Print features dropped by step-wise
            print(f"Features dropped by step-wise for {target_variable}: {set(explainable_variables_filtered_aggregated2) - set(selected_features)}")
            df_engineered2_train, df_engineered2_test = split_data(df_engineered2)  # Split the engineered data
            return_args[reference_pool]['step-wise']['train'][target_variable] = run_and_store_ols_results(df_engineered2_train, target_variable, selected_features)
            return_args[reference_pool]['step-wise']['test_data'][target_variable] = df_engineered2_test  # Save engineered test data

base_df = pd.read_csv(f"Data/processed/features/df_features_raw_refbase.csv")
base_df = base_df[base_df['horizon_label'] <= 30]
base_df['blocks'] = base_df['horizon_label'] * 10
df_pool_counter = base_df.groupby(['pool', 'blocks'])['reference_blockNumber'].count().reset_index()
df_pool_counter_pivot = df_pool_counter.pivot(index='blocks', columns='pool', values='reference_blockNumber')

# Replace `run_args` with your actual dictionary
plot_dataframes(return_args, df_pool_counter_pivot)
plot_dataframes(return_args, df_pool_counter_pivot, adjusted_r_squared=True)
pass