# :chart_with_upwards_trend: Impact of Liquidity Pool Size on Trading Volume in BTC-ETH Pools
Welcome to the official repository for the MSc research project titled "Impact of Liquidity Pool Size on Trading Volume in BTC-ETH Pools", conducted during the Summer 2023 semester. The repository contains all data engineering scripts, models, and additional resources used to support the findings and discussions presented in the final paper.

For a comprehensive understanding of the project, readers are encouraged to access the full research paper available at the following link: [Impact of Liquidity Pool Size on Trading Volume in BTC-ETH Pools](https://drive.google.com/file/d/1TcIuDAD2635uOJd_I_DwPnwqJiMPvlsI/view?usp=drive_link).

## :running: Quick Start

To extract and clean the data from APIs and custom functions, follow these steps:

1. **Set Up Environment**:
    - Ensure your `environment/local-secrets.env` file contains an `ETHERSCAN_KEY` (you can obtain this for free from Etherscan).
    - Verify your configuration settings in `environment/time_spans.py` and `run-config.env`.

2. **Data Sourcing and Cleaning**:
    - Execute the bulk data sourcing and cleaning process by running the script:
      ```
      ./Code/data_sourcing_cleaning/run_source_and_clean.sh
      ```
    - Please note that this step can take some time, especially when downloading Etherscan data due to API limits.
    - For limited time, we offer the source data used for the project at the team's google drive: 
https://drive.google.com/drive/folders/1y5ZwLZK9GQYsCNYSY--4VQMg80dnuwuU?usp=sharing

1. **Feature Engineering**:
    - Run the feature engineering processes in bulk using the script:
      ```
      ./Code/feature_engineering/run_feature_engineering.sh
      ```

2. **Predictive Modeling**:
    - The `Code/predictive_models` directory contains the Ordinary Least Squares (OLS) models. These scripts can be run individually for detailed analysis and experimentation.


## :file_folder: Structure

This repository is organized into several directories:

- **:computer: Code**:

    For detailed script descriptions please refer to the section  - [Detailed script description](#scroll-script-descriptions).

    1. **:hourglass_flowing_sand: Data Sourcing and Cleaning**
        - Contains a dedicated README with instructions and descriptions.
        - Includes three folders for each source (binance, etherscan, uniswap).
        - Handles data sourcing via APIs/web calls (raw data stored in `Data`).
        - Processes data cleaning (results stored in `Data/cleansed`).
    2. **:gear: Feature Engineering**
        - To process the Block Interval Chains and Features of analysis, run the following files in order:
            1. **1_dex_interval_dataframes.py**
            2. **2_dex_direct_pool.py**
            3. **3_cex_spillover.py**
            4. **main.py**
    3. **:bar_chart: EDA**:
        - Produces basic EDA and analysis on the horizons and features.
    4. **:crystal_ball: Predictive Models**
        - **utils/** - Contains functions for data cleaning, model construction, and visualization.
            1. **ols_all_horizons.py**
            2. **ols_best_horizon.py**

- **Other Resources**: Contains self-created diagrams, graphs, and screenshots.

:warning: Data is not versioned controlled in git. Please [for limited time] access data resources (raw, cleansed, processed) in the team's google drive: 
https://drive.google.com/drive/folders/1y5ZwLZK9GQYsCNYSY--4VQMg80dnuwuU?usp=sharing

- **:file_cabinet: Data**
    - [**FeatureDescription.md**](./Code/FeatureDescription.md): Has a breakdown of the features used for the reference paper, and which ones are in-scope for this project.
    - **processed/** Including processed cex_spillovers and direct_pool features when running locally.
    - **processed/features/** Includes final data model writes when running locally.

- **Data** - Holds interim data writes when running locally.

## :scroll: Script Descriptions

| Script Name | Description |
| ----------- | ----------- |
| **1_dex_interval_dataframes** :page_facing_up: | This script performs a range of data processing operations on Uniswap and Etherscan data, conducting cleaning, preprocessing, merging, and analysis tasks. It consolidates mint transactions for optimal interval analysis, infers block intervals, and logs counts of different transaction types. The interim results, including the consolidated DEX data, block data, and interval-based dataframes, are saved for subsequent analysis. The outcome enables a deeper understanding of transaction distributions across pools and types, facilitating further analysis and model building. |
| **2_dex_direct_pool** :page_facing_up: | This script processes data from different pools of a decentralized exchange (DEX) and computes various metrics per transaction hash and interval. It loads previously computed interval dataframes, applies various calculations to extract insights, and consolidates the results into a single dataframe per pool. If run directly, the processed data can be saved into a CSV file. It is part of a larger system for analyzing DEX data. |
| **3_cex_spillover** :page_facing_up: | This script loads, processes, and calculates metrics from the data of a Centralized Exchange (CEX), specifically Binance, relating to transactions' spillover effects on different pools of a decentralized exchange. The script organizes the data into intervals, calculates various metrics for each interval and each pool, and then consolidates the results. If the script is run directly, it can optionally save the processed data to a CSV file. The script is part of a larger system for analyzing CEX and DEX data. |
| **main_py** :page_facing_up: | This Python script serves as the final step of the feature engineering process. It accumulates the volume of each pool (the target variable), merges this with both Centralized Exchange (CEX) and Decentralized Exchange (DEX) feature sets. It then organizes the data into Horizon tables for each pool. The script transforms raw and intermediate data into a form that's ready for use in building predictive models. The output data is saved as a set of CSV files, with each file representing the processed data for a particular pool. |
| **ols_all_horizons** :page_facing_up: | This script performs Ordinary Least Squares (OLS) regression analyses for predicting target variables related to cumulative volumes in different pools of a decentralized exchange. It tests different feature sets and makes predictions for all specified horizons. It stores the metrics of model performance, and finally, it visualizes the results and identifies the top performing models. The script is intended to help understand which features are most predictive of the target variables under different conditions, and can be used to inform feature and model selection in machine learning tasks related to decentralized exchange data. |
| **ols_best_horizon** :page_facing_up: | This script conducts Ordinary Least Squares (OLS) regression analyses in three primary steps: initial analysis, individual run OLS, and all horizons run OLS. The analyses are carried out on datasets from a specific pool of a decentralized exchange to predict cumulative volume. The script consists of the following steps: first_analysis function, individual_run_ols function, and all_horizons_run_ols function. These functions perform various tasks such as replacing nulls, aggregating columns, stepwise selection, and conducting OLS regression, and provide visual representation of data and results. |


## :hammer_and_wrench: Requirements
An ETHERSCAN_KEY is required for API downloads and should be stored in the `environment/local-secrets.env` file.

## :trophy: Results & Analysis
- [Final Report](https://drive.google.com/file/d/1TcIuDAD2635uOJd_I_DwPnwqJiMPvlsI/view?usp=drive_link)
- Final Presentation Slides (TBC)


# :star2: How to Contribute

We welcome contributions from the community. If you're interested in enhancing the analysis, refining models, or adding new features, please feel free to fork the repository and submit a pull request. For major changes, please open an issue first to discuss what you would like to change.

Ensure to update tests as appropriate and adhere to the existing coding style.