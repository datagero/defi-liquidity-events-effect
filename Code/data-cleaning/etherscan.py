import json
import pandas as pd

filepath = "Data/WBTC-WETH_etherscan.json"

# Open the JSON file
with open(filepath) as json_file:
    # Load the data into a Python dictionary
    data = json.load(json_file)

# Convert the dictionary to a DataFrame
df = pd.DataFrame(data.values())

# Save the DataFrame to a CSV file
df.to_csv('Data/cleansed/etherscan.csv', index=False)
