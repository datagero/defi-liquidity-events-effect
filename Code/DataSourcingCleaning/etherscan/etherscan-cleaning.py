import os
import json
import glob
import pandas as pd

# Directory path
out_directory = 'Data/cleansed'

# Check if the directory exists, if not, create it
if not os.path.exists(out_directory):
    os.makedirs(out_directory)

file_pattern = "Data/all_etherscan/WBTC-WETH_etherscan*.json"  # Update the pattern to match your file naming convention
data = {}
# Iterate over the matched files
for file_name in glob.glob(file_pattern):
    with open(file_name, "r") as file:
        # Load the JSON data from each file and append it to the failed_archive list
        ind_data = json.load(file)
        data.update(ind_data)

# Convert the dictionary to a DataFrame
df = pd.DataFrame(data.values())

# Save the DataFrame to a CSV file
df.to_csv(f'{out_directory}/etherscan.csv', index=False)
