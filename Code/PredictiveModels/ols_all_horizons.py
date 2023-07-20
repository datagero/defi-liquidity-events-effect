import pandas as pd
from utils.visualisations import plot_dataframes, dict_to_dataframe, advanced_plot_dataframes
from utils.cols_management import cols_drop_correlated, cols_aggregate_intervals_range
import utils.ols as ols
import itertools



target_variables = ['cum_volume_500', 'cum_volume_3000', 'cum_volume_base']

run_args = {
    3000: target_variables,
    500: target_variables
}
return_args = {}

print("All Inscope combinations for target_variables and run_args:")
print("Reference Pool, Target Variable")
for arg_value, arg_targets in run_args.items():
    combinations = list(itertools.product([arg_value], arg_targets))
    print(combinations)


all_figs = []

remove_list = ['vol_0_1', 'vol_0_2', 'vol_0_3', 'avg-USD-iother_01', 'rate-USD-iother_01']


def train_and_predict_for_all_horizons(df, target_variable, explainable_variables):
    """Run OLS for all horizons and store results in the dictionary."""
    models, train_metrics, test_metrics, _, _ = ols.cross_validate_for_all_horizons(df, target_variable, explainable_variables, k=5)
    return models, train_metrics, test_metrics

for reference_pool, target_variables in run_args.items():
    return_args[reference_pool] = {}
    return_args[reference_pool]['all_features'] = {'train': {}, 'test': {}}
    return_args[reference_pool]['reduced_multicollinearity'] = {'train': {}, 'test': {}}
    return_args[reference_pool]['step-wise'] = {'train': {}, 'test': {}}
    for target_variable in target_variables:
        print("\n=========================================================")
        print("Processing: {}-{}".format(reference_pool, target_variable))

        if 'all_features' in return_args[reference_pool]:
            df_raw = pd.read_csv(f"Data/processed/features/df_features_raw_ref{reference_pool}.csv")
            df, explainable_variables_filtered = ols.prepare_dataframe(df_raw, remove_list) # Exclusive for "all_features" model - A standard remove list due to multicollinearity and high number of nulls
            models, train_metrics, test_metrics = train_and_predict_for_all_horizons(df, target_variable, explainable_variables_filtered)
            return_args[reference_pool]['all_features']['train'][target_variable] = train_metrics
            return_args[reference_pool]['all_features']['test'][target_variable] = test_metrics
        if 'reduced_multicollinearity' in return_args[reference_pool]:
            # Further feature engineering
            df_raw2 = pd.read_csv(f"Data/processed/features/df_features_raw_ref{reference_pool}.csv")
            df2, _ = ols.prepare_dataframe(df_raw2, [])
            df2_engineered, explainable_variables_filtered_aggregated2 = ols.prepare_dataframe_engineered(df2, cols_drop_correlated, cols_aggregate_intervals_range)
            models, train_metrics, test_metrics = train_and_predict_for_all_horizons(df2_engineered, target_variable, explainable_variables_filtered_aggregated2)
            return_args[reference_pool]['reduced_multicollinearity']['train'][target_variable] = train_metrics
            return_args[reference_pool]['reduced_multicollinearity']['test'][target_variable] = test_metrics
        if 'step-wise' in return_args[reference_pool]:
            # Step-wise feature selection
            df_raw3 = pd.read_csv(f"Data/processed/features/df_features_raw_ref{reference_pool}.csv")
            df3, _ = ols.prepare_dataframe(df_raw3, [])
            df3_engineered, explainable_variables_filtered_aggregated3 = ols.prepare_dataframe_engineered(df3, cols_drop_correlated, cols_aggregate_intervals_range)
            selected_features3 = ols.stepwise_selection(df3_engineered, df3_engineered[target_variable], explainable_variables_filtered_aggregated3)
            # Print features dropped by step-wise
            print(f"Features dropped by step-wise for {target_variable}: {set(explainable_variables_filtered_aggregated3) - set(selected_features3)}")
            models, train_metrics, test_metrics = train_and_predict_for_all_horizons(df3_engineered, target_variable, selected_features3)
            return_args[reference_pool]['step-wise']['train'][target_variable] = train_metrics
            return_args[reference_pool]['step-wise']['test'][target_variable] = test_metrics

# Iterim, get rough count of train and tests datasets per pool
base_df = pd.read_csv(f"Data/processed/features/df_features_raw_refbase.csv")
base_df = base_df[base_df['horizon_label'] <= 30]
base_df['blocks'] = base_df['horizon_label'] * 10

df_train, df_test = ols.split_data(base_df, test_size=0.2)
train_df_pool_counter = df_train.groupby(['pool', 'blocks'])['reference_blockNumber'].count().reset_index()
test_df_pool_counter = df_test.groupby(['pool', 'blocks'])['reference_blockNumber'].count().reset_index()

df_pool_counter_pivot = {
    'train': train_df_pool_counter.pivot(index='blocks', columns='pool', values='reference_blockNumber'),
    'test': test_df_pool_counter.pivot(index='blocks', columns='pool', values='reference_blockNumber')
    }

# Build a dataframe from modelling results
df = dict_to_dataframe(return_args)

# Plot the results
advanced_plot_dataframes(df, df_pool_counter_pivot, r2_col='r2', output_path="Other Resources/OLS_allhorizons_r2.png")
advanced_plot_dataframes(df, df_pool_counter_pivot, r2_col='r2_adj', output_path="Other Resources/OLS_allhorizons_r2adj.png")

# Ad-hoc: Find the top 3 R2:horizon combinations for each run_type
top_r2 = df.groupby(['pool', 'split_type', 'run_type', 'target_variable']).apply(lambda x: x.nlargest(3, 'r2')).reset_index(drop=True)
top_r2

pass

