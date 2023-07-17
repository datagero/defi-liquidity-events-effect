import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import math

def print_heatmap(corr_matrix, title: str):

    plt.figure(figsize=(12, 10))  # Adjust the figure size as per your preference

    sns.heatmap(corr_matrix, annot=False, cmap="coolwarm", linewidths=0.5, cbar=True)

    plt.xticks(rotation=90, fontsize=8)  # Rotate and adjust the font size of the X-axis labels
    plt.yticks(fontsize=8)  # Adjust the font size of the Y-axis labels
    plt.title(title, fontsize=10)  # Adjust the font size of the title

    plt.tight_layout()  # Automatically adjust the spacing between subplots

    plt.show()


def print_highest_corr(corr_matrix):
    # Find the variables with the highest correlation
    highest_corr_variables = corr_matrix.abs().unstack().sort_values(ascending=False)

    # Print the top correlated variables (excluding self-correlations)
    top_correlations = highest_corr_variables[highest_corr_variables < 1.0]
    print(top_correlations[:15])


def get_values_from_tuple(tuple_data, i):
    return [item[i] for item in tuple_data]

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


