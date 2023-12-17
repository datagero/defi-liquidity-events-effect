#!/bin/bash

# Make sure the script is executable. Run the script from the root directory of the project.
chmod +x ./Code/feature_engineering/run_feature_engineering.sh

# Stop the script if any command fails
set -e

# Run the Python scripts in the specified order
python Code/feature_engineering/1_dex_interval_dataframes.py
python Code/feature_engineering/2_dex_direct_pool.py
python Code/feature_engineering/3_cex_spillover.py
python Code/feature_engineering/main.py
