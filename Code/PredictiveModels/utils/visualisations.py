import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import math
from matplotlib.lines import Line2D

def print_heatmap(corr_matrix, title: str):

    plt.figure(figsize=(12, 10))  # Adjust the figure size as per your preference

    sns.heatmap(corr_matrix, annot=False, cmap="coolwarm", linewidths=0.5, cbar=True)

    plt.xticks(rotation=90, fontsize=8)  # Rotate and adjust the font size of the X-axis labels
    plt.yticks(fontsize=8)  # Adjust the font size of the Y-axis labels
    plt.title(title, fontsize=10)  # Adjust the font size of the title

    plt.tight_layout()  # Automatically adjust the spacing between subplots

    plt.show()

def print_heatmap_list(corr_matrices, titles, output_path=None):
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(20, 8))

    cmaps = ['coolwarm', 'coolwarm']

    for ax, matrix, title, cmap in zip(axes.flatten(), corr_matrices, titles, cmaps):
        sns.heatmap(matrix, ax=ax, cmap=cmap)
        ax.xaxis.set_tick_params(rotation=90)
        ax.yaxis.set_tick_params(rotation=0)
        ax.set_title(title, fontsize=15)  # Set the title for each subplot

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path)
    plt.show()


def print_highest_corr(corr_matrix):
    # Find the variables with the highest correlation
    highest_corr_variables = corr_matrix.abs().unstack().sort_values(ascending=False)

    # Print the top correlated variables (excluding self-correlations)
    top_correlations = highest_corr_variables[highest_corr_variables < 1.0]
    print(top_correlations[:15])


def get_values_from_tuple(tuple_data, i):
    return [item[i] for item in tuple_data]


def residuals_horizons(test_data, test_predictions, horizons, title='Residuals for all Horizons'):
    # Residual analysis
    residuals_all_horizons = []
    for test, pred in zip(test_data, test_predictions):
        X_test, y_test = test
        residuals = y_test - pred
        residuals_all_horizons.append(residuals)

    # Plot residuals
    plt.figure(figsize=(10, 6))
    for horizon, residuals in zip(horizons, residuals_all_horizons):
        plt.scatter([horizon]*len(residuals), residuals, alpha=0.3)
    plt.xlabel('Horizon (blocks)')
    plt.ylabel('Residuals')
    plt.title(title)
    plt.savefig("Other Resources/residuals_all_horizons.png")
    plt.show()


def residuals_individual(model, last_test_data, last_test_predictions, log_target=True, title='Residuals'):

    from sklearn.metrics import mean_squared_error, mean_absolute_error
    import numpy as np
    # Compute RMSE and MAE for the last fold
    X_test_updated, y_test_updated = last_test_data
    y_test_pred_updated = last_test_predictions

    if log_target:
        y_test_updated_original = 10 ** y_test_updated
        y_test_pred_updated_original = 10 ** y_test_pred_updated

        rmse_updated = np.sqrt(mean_squared_error(y_test_updated_original, y_test_pred_updated_original))
        mae_updated = mean_absolute_error(y_test_updated_original, y_test_pred_updated_original)
        label = 'Predicted log10(cum_volume_500)'

    else:
        rmse_updated = np.sqrt(mean_squared_error(y_test_updated, y_test_pred_updated))
        mae_updated = mean_absolute_error(y_test_updated, y_test_pred_updated)
        label = 'Predicted cum_volume_500'

    # Extract feature importance for the model
    feature_importance_updated = model.params[1:]  # Exclude the intercept

    rmse_updated, mae_updated, feature_importance_updated

    import matplotlib.pyplot as plt
    # Residuals Analysis
    # Residuals are the difference between the actual values and the predicted values
    residuals = y_test_updated - y_test_pred_updated

    plt.figure(figsize=(10, 6))
    plt.scatter(range(len(residuals)), residuals, alpha=0.5, marker='x', label='Residuals')
    plt.axhline(y=0, color='r', linestyle='--')
    plt.xlabel(label)
    plt.ylabel('Residuals')
    plt.title(title)
    plt.savefig("Other Resources/residuals.png")
    plt.show()


def dict_to_dataframe(dict_data):
    data = {'pool': [], 'r2': [], 'r2_adj': [], 'horizon': [], 'run_type': [], 'split_type': [], 'target_variable': []}
    for i, (reference_pool, run_types) in enumerate(dict_data.items()):
        for j, (run_type, split_types) in enumerate(run_types.items()):
            for k, (split_type, target_variables) in enumerate(split_types.items()):
                for target_variable in target_variables:
                    model_returns = dict_data[reference_pool][run_type][split_type][target_variable]
                    r_squared_adj_values = get_values_from_tuple(model_returns, 3)
                    r_squared_values = get_values_from_tuple(model_returns, 2)
                    horizon_values = get_values_from_tuple(model_returns, 0)

                    data['pool'] += [reference_pool] * len(horizon_values)
                    data['r2'] += r_squared_values
                    data['r2_adj'] += r_squared_adj_values
                    data['horizon'] += horizon_values
                    data['run_type'] += [run_type] * len(horizon_values)
                    data['split_type'] += [split_type] * len(horizon_values)
                    data['target_variable'] += [target_variable] * len(horizon_values)
            
    df = pd.DataFrame(data)
    return df


def advanced_plot_dataframes(metrics_df, df_pool_counter_dict, r2_col='r2', output_path=None):
    # Get unique pools, split types, and run types
    pools = metrics_df['pool'].unique()
    split_types = metrics_df['split_type'].unique()
    run_types = metrics_df['run_type'].unique()
    target_variables = metrics_df['target_variable'].unique()

    r2_label = {'r2': 'R-squared', 'r2_adj': 'R-squared (adjusted)'}[r2_col]

    # Initialize a figure
    fig, axs = plt.subplots(1, len(pools) * len(split_types), figsize=(16, 6))

    # Initialize a counter for the current subplot
    count = 0

    # Define markers for different target variables
    markers = ['o', 'v', '^']

    # Define color palette for different run types
    palette = sns.color_palette("Paired", len(run_types))
    pallete_target = sns.color_palette("Set2", len(target_variables))
    pallete_pools = sns.color_palette("husl", len(pools))

    # For each split type
    for j, split_type in enumerate(split_types):
        # For each pool
        for i, pool in enumerate(pools):
        
            # Determine the current axes for the subplot
            ax = axs[count]
            ax.set_ylim(-0.4, 1)

            # Filter data for the current pool and split type
            pool_split_data = metrics_df[(metrics_df['pool'] == pool) & (metrics_df['split_type'] == split_type)]
            df_pool_counter = df_pool_counter_dict[split_type]

            ax2 = ax.twinx()
            df_pool_counter = df_pool_counter_dict[split_type]
            bottom = np.zeros(len(df_pool_counter))
            for h, pool_counter in enumerate(df_pool_counter.columns):
                ax2.bar(df_pool_counter.index, df_pool_counter[pool_counter], bottom=bottom, alpha=0.3, label=f'Pool {pool_counter}', width=8, color=pallete_pools[h])
                bottom += df_pool_counter[pool_counter]
            ax2.legend()
            # Add label to x-axis (only in the middle subplot)
            if count == (len(pools) * len(split_types)) // 2:
                ax.set_xlabel('Horizon')
            else:
                ax.set_xticklabels([])

            # Add label to y-axis (only in the first subplot of each pool)
            if count % len(pools) == 0:
                ax2.set_ylabel("Transaction Count")

            # Create a line plot for R^2 against horizon for each target variable
            for l, target_variable in enumerate(target_variables):
                target_data = pool_split_data[pool_split_data['target_variable'] == target_variable]
                sns.lineplot(data=target_data, x='horizon', y=r2_col, ax=ax, label=target_variable, legend=False, color=pallete_target[l])

                # Add markers for the top 3 R^2 values for each run type
                for k, run_type in enumerate(run_types):
                    run_type_target_data = target_data[target_data['run_type'] == run_type]
                    top3_run_type_target_data = run_type_target_data.nlargest(3, r2_col)  # Get the top 3 R2 values
                    ax.scatter(top3_run_type_target_data['horizon'], top3_run_type_target_data[r2_col], 
                               marker=markers[l % len(markers)], 
                               color=palette[k], 
                               label=f'Top 3 {run_type}')  # Plot top 3 R2 values for each run type with different markers

            # Set title
            ax.set_title(f'Pool {pool} - {split_type}', pad=20)

            # Increment counter
            count += 1

    # Create a consolidated legend outside the subplots
    target_legend_handles = []
    for g, target_variable in enumerate(target_variables):
        target_legend_handles.append(Line2D([], [], color=pallete_target[g], label=f'Target Variable: {target_variable}'))

    type_legend_handles = []
    for run_type in run_types:
        label = f'Top 3 {run_type}'
        marker = markers[run_types.tolist().index(run_type) % len(markers)]
        type_legend_handles.append(Line2D([0], [0], marker=marker, color='w', label=label, markerfacecolor='black',
                                        markersize=8))

    # Create separate legends for target variables and run types
    fig.legend(handles=target_legend_handles, loc='upper center', ncol=len(target_variables), fontsize=8)
    fig.legend(handles=type_legend_handles, loc='lower center', ncol=len(run_types), fontsize=8)

    # Adjust layout for better appearance
    fig.tight_layout(rect=[0, 0.1, 1, 0.9])
    if output_path:
        plt.savefig(output_path)
    plt.show()



def plot_dataframes(return_args, df_pool_counter_dict, adjusted_r_squared=False):
    num_pools = len(return_args)
    max_run_types = max(len(run_types) for run_types in return_args.values())
    max_split_types = max(len(split_types) for run_types in return_args.values() for split_types in run_types.values())

    fig, axs = plt.subplots(num_pools*max_split_types, max_run_types, figsize=(10*max_run_types, 6*num_pools*max_split_types))


    for i, (reference_pool, run_types) in enumerate(return_args.items()):
        for j, (run_type, split_types) in enumerate(run_types.items()):
            for k, (split_type, target_variables) in enumerate(split_types.items()):
                ax = axs[i*max_split_types + k, j]

                ax2 = ax.twinx()
                df_pool_counter = df_pool_counter_dict[split_type]
                bottom = np.zeros(len(df_pool_counter))
                for pool in df_pool_counter.columns:
                    ax2.bar(df_pool_counter.index, df_pool_counter[pool], bottom=bottom, alpha=0.3, label=f'Pool {pool}', width=8)
                    bottom += df_pool_counter[pool]

                for target_variable in target_variables:
                    model_returns = return_args[reference_pool][run_type][split_type][target_variable]
                    if adjusted_r_squared:
                        r_squared_values = get_values_from_tuple(model_returns, 3)
                        r2_label = 'R-squared (adjusted)'
                    else:
                        r_squared_values = get_values_from_tuple(model_returns, 2)
                        r2_label = 'R-squared'
                    horizon_values = get_values_from_tuple(model_returns, 0)
                    ax.plot(horizon_values, r_squared_values, label=target_variable)

                ax.set_xlabel('Horizon (blocks)')
                ax.set_ylabel(r2_label)
                ax.set_ylim(0, 1)
                ax.legend(loc='upper left')

                ax2.set_ylabel('Observation Count')
                ax2.set_ylim(0, df_pool_counter.sum(axis=1).max() + 1000)
                ax2.legend(loc='upper right')

                ax.set_title(f'{split_type} - Reference pool: {reference_pool} - {run_type}', fontsize=14)

    plt.tight_layout()
    plt.show(block=False)


