import json
import glob
import pandas as pd

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
df.to_csv('Data/cleansed/etherscan.csv', index=False)
