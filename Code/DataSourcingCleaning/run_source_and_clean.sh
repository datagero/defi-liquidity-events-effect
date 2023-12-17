#!/bin/bash

# Make sure the script is executable. Run the script from the root directory of the project.
chmod +x ./Code/DataSourcingCleaning/run_source_and_clean.sh

# Stop the script if any command fails
set -e

# Run the Python scripts in the specified order
# Run the Python scripts in the specified order
echo "Running Uniswap API script..."
python Code/DataSourcingCleaning/uniswap/api_uniswap.py

echo "Running Etherscan API script..."
python Code/DataSourcingCleaning/etherscan/api_etherscan.py

echo "Running Binance download script..."
python Code/DataSourcingCleaning/binance/download-trade.py -t "spot" -s "ETHBTC" -skip-monthly 1

echo "Running Uniswap cleaning script..."
python Code/DataSourcingCleaning/uniswap/uniswap-cleaning.py

echo "Running Etherscan cleaning script..."
python Code/DataSourcingCleaning/etherscan/etherscan-cleaning.py

echo "Running Binance cleaning script..."
python Code/DataSourcingCleaning/binance/binance-cleaning.py

echo "All Sourcing and Cleaning scripts completed successfully."
