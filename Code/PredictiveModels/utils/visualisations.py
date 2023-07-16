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
    nrows = math.ceil(num_figs / 2.0)  # Adjust this value to change the number of rows
    ncols = 2  # Two plots per row

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

        plt.title(f'Mint of reference on Pool: {fig["reference_pool"]}')
        plt.suptitle('R-squared and Observation Count vs. Horizon')
    
    plt.tight_layout()
    plt.show()