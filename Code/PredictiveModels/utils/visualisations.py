import matplotlib.pyplot as plt
import seaborn as sns
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


def load_multiple_r2_figs(r_squared_values, horizon_values, observation_counts, reference_pool):
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    ax1.scatter(horizon_values, r_squared_values, color='b')
    ax1.set_xlabel('Horizon (blocks)')
    ax1.set_ylabel('R-squared', color='b')
    ax1.set_ylim(0, 1)  # Set the y-axis range for R-squared

    ax2.plot(horizon_values, observation_counts, color='r')
    ax2.set_ylabel('Observation Count', color='r')
    ax2.set_ylim(0, max(observation_counts) + 1000)  # Set the y-axis range for observation count

    plt.title('R-squared and Observation Count vs. Horizon')
    plt.suptitle(f'Mint of reference on Pool: {reference_pool}')
    # plt.text(0.5, 0.95, f'Best horizon R2: {horizon_values[r_squared_values.index(max(r_squared_values))]}', ha='center', va='center', transform=plt.gca().transAxes, fontsize=10)
    return fig
    #plt.show(block=False)


def display_figures(figures):
    num_figs = len(figures)
    nrows = math.ceil(num_figs / 6)  # Adjust this value to change the number of rows
    ncols = 6  # Two plots per row

    fig, axs = plt.subplots(nrows, ncols, figsize=(15, 5*nrows))
    axs = axs.flatten()  # Flatten the array of axes to easily iterate over
    
    for idx, fig in enumerate(figures):
        ax1 = axs[idx]
        ax2 = ax1.twinx()
        
        ax1.scatter(fig["horizon_values"], fig["r_squared_values"], color='b')
        ax1.set_xlabel('Horizon (blocks)')
        ax1.set_ylabel('R-squared', color='b')
        ax1.set_ylim(0, 1)  # Set the y-axis range for R-squared

        ax2.plot(fig["horizon_values"], fig["observation_counts"], color='r')
        ax2.set_ylabel('Observation Count', color='r')
        ax2.set_ylim(0, max(fig["observation_counts"]) + 1000)  # Set the y-axis range for observation count

        plt.title(f'{fig["reference_pool"]}', fontsize=8)
        plt.suptitle('R-squared and Observation Count vs. Horizon')
    
    plt.tight_layout()
    plt.show()

import numpy as np
def plot_dataframes(return_args, df_pool_counter):

    for reference_pool, target_variables in return_args.items():
        # Initialize a new figure for the current reference pool
        fig, ax1 = plt.subplots(figsize=(10, 6))
        ax2 = ax1.twinx()

        for target_variable in target_variables:
            r_squared_values = return_args[reference_pool][target_variable]['all_features'][0]
            horizon_values = return_args[reference_pool][target_variable]['all_features'][2]

            # Plot r_squared_values for this target variable
            ax1.plot(horizon_values, r_squared_values, label=target_variable)

        # Plot the observation counts for this reference pool
        bottom = np.zeros(len(df_pool_counter))  # Initialize the bottom values to 0 for the first layer of the stack

        for pool in df_pool_counter.columns:
            ax2.bar(df_pool_counter.index, df_pool_counter[pool], bottom=bottom, alpha=0.3, label=f'Pool {pool}', width=8)
            bottom += df_pool_counter[pool]  # Update the bottom values for the next layer

        # Set labels, title and limits for the plot
        ax1.set_xlabel('Horizon (blocks)')
        ax1.set_ylabel('R-squared')
        ax1.set_ylim(0, 1)  # Set the y-axis range for R-squared
        ax1.legend(loc='upper left')

        ax2.set_ylabel('Observation Count')
        ax2.set_ylim(0, df_pool_counter.sum(axis=1).max() + 1000)  # Set the y-axis range for observation count

        # ax2.set_ylim(0, df_pivot.values.max()*2)  # Set the y-axis range for observation count
        ax2.legend(loc='upper right')

        plt.title(f'Reference pool: {reference_pool}', fontsize=14)
        plt.tight_layout()
        plt.show(block=False)