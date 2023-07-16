import os
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
import seaborn as sns
from statsmodels.stats.outliers_influence import variance_inflation_factor

processed_results_dir = "Data/processed"
target_variable = 'cum_volume_3000_ref500'
explainable_variables = ['s0', 'w0', 'blsame_1', 'slsame_1', 'wlsame_1', 'blsame_2', 'slsame_2', 'wlsame_2', 'blsame_3', 'slsame_3', 'wlsame_3', 'vol_0_1', 'vol_0_2', 'vol_0_3', 'rate-USD-isame_01', 'rate-USD-isame_12', 'rate-USD-isame_23', 'rate-count-isame_01', 'rate-count-isame_12', 'rate-count-isame_23', 'avg-USD-isame_01', 'avg-USD-isame_12', 'avg-USD-isame_23', 'blother_1', 'slother_1', 'wlother_1', 'blother_2', 'slother_2', 'wlother_2', 'blother_3', 'slother_3', 'wlother_3', 'rate-USD-iother_01', 'rate-USD-iother_12', 'rate-USD-iother_23', 'rate-count-iother_01', 'rate-count-iother_12', 'rate-count-iother_23', 'avg-USD-iother_01', 'avg-USD-iother_12', 'avg-USD-iother_23']

# Read df
df = pd.read_csv(os.path.join(processed_results_dir, "df.csv"), index_col=0)


# helper function
def print_heatmap(df, title:str):
    # correlation after removing multicollinearity
    cols_sorted = df.columns.sort_values()
    df = df[cols_sorted]
    corr_matrix = df.corr()

    plt.figure(figsize=(8, 6))  # Adjust the figure size as per your preference

    sns.heatmap(corr_matrix, annot=False, cmap="coolwarm", linewidths=0.5, cbar=True)

    plt.xticks(rotation=90, fontsize=8)  # Rotate and adjust the font size of the X-axis labels
    plt.yticks(fontsize=8)  # Adjust the font size of the Y-axis labels
    plt.title(title, fontsize=10)  # Adjust the font size of the title
    plt.show()



# Train model on horizon 20
subset_data = df[df['horizon_label'] == 20]
X = subset_data[explainable_variables]
y = subset_data[target_variable]


# Remove the rows that contain null values
null_rows = X[X.isnull().any(axis=1)].index
X = X.drop(null_rows, axis=0)
y = y.drop(null_rows, axis=0)

# Fit the OLS model
model = sm.OLS(y, X)
results = model.fit()

# Retrieve the R-squared value
r_squared = results.rsquared

# identify multicollinearity
vif = pd.DataFrame()
vif["Variable"] = X.columns
vif["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]

# remove cols with multicollinearity
X_red = X.drop(columns = vif[vif['VIF'] > 10]['Variable'])

title_before = "Correlation Matrix of Independent Variables - Before Multicollinearity"
title_after = "Correlation Matrix of Independent Variables - After Multicollinearity"

print_heatmap(X, title_before)

print_heatmap(X_red, title_after)
