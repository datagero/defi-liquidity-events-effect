import os
import zipfile
import pandas as pd

# Define directory with zip files and destination for extraction
zip_dir_path = 'Code/binance/data/spot/daily/trades/ETHBTC'
extract_to_path = 'Data/binance-extracts'

# Define date scope
scope_date = ["2023-06-19", "2023-06-20"]

# Initialize a list to store DataFrames
dfs = []

# Iterate over each file in directory
for file_name in os.listdir(zip_dir_path):
    # Check if file is a zip file and in scope_date
    if file_name.endswith('.zip') and any(date in file_name for date in scope_date):
        # Create full file path
        file_path = os.path.join(zip_dir_path, file_name)

        # Open zip file
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # Extract all files to specified path
            zip_ref.extractall(extract_to_path)

# Find csv files in the extracted files
for file in os.listdir(extract_to_path):
    if file.endswith(".csv"):
        csv_path = os.path.join(extract_to_path, file)

        # Load the CSV file and append DataFrame to the list
        dfs.append(pd.read_csv(csv_path, names=['id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker', 'isBestMatch']))

# Concatenate all DataFrames in the list along the column axis
df = pd.concat(dfs, axis=0)


# Save the DataFrame to a single CSV file
df.to_csv('Data/binance.csv', index=False)
