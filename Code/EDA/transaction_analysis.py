import os
import pickle
import pytz
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

key_dates = {
    'terra_luna': '2022-05-12',
    'the_merge': '2022-09-06',
}
# Create a dictionary to store the colors for each key date
colors = {
    'the_merge': 'red',
    'terra_luna': 'blue',
}

CLEANSED_FILEPATH = "DataIterim/cleansed"
RESULTS_DIR = "DataIterim/interim_results"

df_blocks = pd.read_csv(os.path.join(RESULTS_DIR, "df_blocks.csv"))
df_blocks_full = pd.read_csv(os.path.join(RESULTS_DIR, "df_blocks_full.csv"))



block_counts = df_blocks_full['pool'].value_counts().reset_index()
block_counts.columns = ['Pool', 'Block Count']

def get_interval_metrics(data):

    pool_ranges = {}

    for pool, pool_data in data.items():
        interval_ranges = {}

        for category_data in pool_data.values():
            for interval_data in category_data.values():
                for interval, interval_metrics in interval_data.items():
                    if interval not in interval_ranges:
                        interval_ranges[interval] = {}
                        interval_ranges[interval]['blockTime'] = {'min': float('inf'), 'max': float('-inf')}
                        interval_ranges[interval]['row_count'] = {'min': float('inf'), 'max': float('-inf')}

                    blocktime = interval_metrics.get('blockTime')
                    if blocktime:
                        if blocktime < interval_ranges[interval]['blockTime']['min']:
                            interval_ranges[interval]['blockTime']['min'] = blocktime
                        if blocktime > interval_ranges[interval]['blockTime']['max']:
                            interval_ranges[interval]['blockTime']['max'] = blocktime

                    df = interval_metrics.get('df')
                    if not df.empty:
                        size = df.shape[0]
                        if size == 1115:
                            pass
                        if size < interval_ranges[interval]['row_count']['min']:
                            interval_ranges[interval]['row_count']['min'] = size
                        if size > interval_ranges[interval]['row_count']['max']:
                            interval_ranges[interval]['row_count']['max'] = size

        pool_ranges[pool] = interval_ranges

    return pool_ranges

with open(os.path.join(RESULTS_DIR, "interval_dataframes.pickle"), "rb") as pickle_file:
    data = pickle.load(pickle_file)

metrics = get_interval_metrics(data)
print(metrics)



def analyse_transaction_counts():
    """
    Analyse the number of transactions per day for each pool and transaction type.
    Note - this uses the df_reduced which excludes >1 mint operations if they belong to the same block.
    """
    results_dir = RESULTS_DIR
    df_reduced = pd.read_csv(os.path.join(results_dir, "df_reduced.csv"))

    df_reduced['timestamp'] = pd.to_datetime(df_reduced['timestamp'])
    df_reduced['date'] = df_reduced['timestamp'].dt.date

    # Group the data by timestamp and pool and count the number of transactions
    grouped = df_reduced.groupby(['date', 'pool', 'transaction_type']).size().reset_index(name='transaction_count')

    # Get unique pool values
    pools = grouped['transaction_type'].unique()

    # Create subplots with the same x-axis range
    fig, axes = plt.subplots(len(pools), 1, figsize=(10, 8), sharex=True)

    # Iterate over each pool and plot the transaction counts by date
    for i, transaction_type in enumerate(pools):
        # Filter data for the current transaction type
        transaction_type_data = grouped[grouped['transaction_type'] == transaction_type]
        
        # Create a new y-axis for each subplot
        ax = axes[i].twinx()
        
        # Iterate over each pool and plot the transaction counts by date
        for pool, data in transaction_type_data.groupby('pool'):
            ax.plot(data['date'], data['transaction_count'], label=pool)
        
        # Set plot title and labels
        ax.set_title(f'Transaction Type {transaction_type}')
        ax.set_ylabel('Transaction Count')
        ax.legend(title='Pool')

    # Set common x-axis label
    axes[-1].set_xlabel('Date')

    # Iterate over each subplot and draw vertical lines at key dates
    for ax in axes:
        for label, date in key_dates.items():
            date_obj = pd.to_datetime(date).tz_localize(None)  # Convert date to datetime object without timezone
            ax.axvline(date_obj, color=colors[label], linestyle='--')

    # Create a general legend for the labels
    legend_elements = [Line2D([0], [0], color=colors[label], linestyle='--', label=label) for label in key_dates.keys()]
    fig.legend(handles=legend_elements, title='Key Dates', loc='upper left')

    # Adjust spacing between subplots
    plt.tight_layout()

    # Save the plot
    plt.savefig(os.path.join("Other Resources", "transaction_counts.png"))

    # Display the plot
    plt.show()



if __name__ == "__main__":
    analyse_transaction_counts()