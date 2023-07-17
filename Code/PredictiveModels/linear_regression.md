import pandas as pd

# Read direct_pool.csv
df_direct_pool = pd.read_csv('Data/processed/direct_pool.csv', index_col=0)


#possible prediction horizons are
#measured in number of blocks mined on the blockchain


# our target variables can either relate to 
# - the cumulative incoming volume on the same pool as the one where the mint operation of reference is executed
# - on the other pool, or 
# in aggregation on both pools.

# We consider the following target variables:
# cumulative trading volume y in USD on pool 500




#  new provision of liquidity (i.e. ticking of our trading clock)
# We test forecasting horizons every ten blocks up to the next mint operation on either pool









#we also test for different horizons that widen ten blocks each time, and that can
# theoretically lengthen up to the maximum time between which two consecutive mint
# operations on either pool are recorded on the blockchain






#We now proceed to a detailed investigation of our forecasting ability regarding the
#incoming trading volume on pool 500, after a mint operation occurred on pool 3000.



#we now pursue step-wise feature selection. We choose a target horizon of 120
#blocks by examining the R2 values on the test set in our chosen framework
# Step-wise feature selection is performed by iteratively adding and removing features






##Visualization
#decay of the distribution of number of data points available per
#horizon, which is shown on the secondary axis of both plots of Fig. 4. 
# The stacked histograms are restricted to a maximum horizon of 300 blocks, and they are coloured
# differently according to whether the mint operation of reference occurred on pool 500
# or pool 3000.








#start with p distinct predictors and assume that there is approximately a linear relationship between the predictors and the response Y.
#The linear model takes the form
#Y ≈ β0 + β1X1 + β2X2 + · · · + βpXp

#we compute estimates βˆ0, βˆ1, ..., βˆp of the coefficients of the model via the least-squares approach
#Y ≈ βˆ0 + βˆ1X1 + βˆ2X2 + · · · + βˆpXp

# we also need to test for the hypothesis that there is no relationship
# between the predictors and the response. This is done by computing the F -statistic
# F = (TSS − RSS)/p
# RSS/(n − p − 1)

# where TSS = ∑(yi − y¯)2 is the total sum of squares, and RSS = ∑(yi − fˆ(xi))2 is the residual sum of squares. 

# If the F -statistic is larger than 1 but its p-value is small, then we conclude that at least one βj is indeed non-zero





# we assess the lack of fit of the model via the root mean squared error
# RMSE = qRSS n , and similarly gauge the extent to which the model fits the data by
# computing its R2 coefficient
# R2 = TSS − RSS

# Since R2 always increases when more variables are added to
# the model, it is often useful to compare such models by considering the adjusted R2
# R2adj = 1 − nn −− p1(1 − R2).



## Collinearity
# The variance inflation factor (VIF) is the ratio of the variance of βˆj when fitting the
# full model divided by the variance of βˆj if fit on its own. That is,
# VIF (βˆj ) = Var(βˆj )/Var(βˆj |X1, . . . , Xj−1,Xj+1, . . . , Xp)
# A VIF value that is greater than 5 or 10 indicates a problematic amount of collinearity.

#the majority of our factors have a VIF value close to 1, with only a few cases that
# reach VIF = 7.5 but that we allow.

# Collinearity reduces the accuracy of the estimates of the regression coefficients, since it becomes
# difficult to separate out the individual effects of the concerned variables. Similarly,
# collinearity also causes inaccuracies in the t-statistic of the related variables, where
# this measure assesses the significance and level of contribution of each feature to the
# regression.

# The few critical instances of high correlation are dealt
# with by first dropping the b2same, b3same, b2other, b3other, vol01, vol02, TVL3000-
# 500, and binance-count-(01, 12, 23) variables.


# aggregate the rates of concern to the longer 03 intervals, 
# i.e. we compute rate-count-03same, rate-count-03other and
# binance-btc-03, and drop the features related to the respective sub-ranges.


# sum eth500-USD-03 and eth3000-USD-03 into a new feature ethTot-USD-03, and
# similarly we calculate btcTot-USD-03. These modifications result in a final set of 41
# features to consider in our model.












# If the linear model assumptions are correct, then one can show that
# E(RSS/(n − p − 1)) = σ2
# E(TSS/(p)) = σ2 + ∑(βj)^2
# where σ2 = Var(ε) is the variance of the error term ε. Consequently, if the linear
# model assumptions are correct, then we expect F to take on a value close to 1.
# On the other hand, if H0 is false, then we expect F to be greater than 1.

# The F -statistic can be easily computed once we have the ANOVA table, which
# is a table that contains the RSS and TSS values needed to compute F . The ANOVA
# table for the linear regression of Y onto X1, . . . , Xp is given by
# Source df SS MS F
# Regression p RSS RSS/p F = MS/MSR
# Residual n − p − 1 TSS − RSS RSS/(n − p − 1)
# Total n − 1 TSS









# best horizon for marginal gain r-squared

# Balance R-squared and Observation Counts: Find a balance between R-squared and observation counts. 
# A higher R-squared with a reasonable number of observations generally indicates a stronger model. 
# However, a high R-squared with a very low observation count may indicate overfitting, 
# where the model is fitting the noise or idiosyncrasies of the limited data rather than capturing meaningful patterns.

# def select_model(r_squared_values, observation_counts):
#     if len(r_squared_values) != len(observation_counts):
#         raise ValueError("Lengths of input lists must match.")

#     best_ratio = r_squared_values[1] / observation_counts[1]  # Initialize with the second model
#     best_model_index = 1

#     for i in range(2, len(r_squared_values)):  # Start from the second model
#         ratio = r_squared_values[i] / observation_counts[i]
#         if ratio > best_ratio:
#             best_ratio = ratio
#             best_model_index = i

#     best_r_squared = r_squared_values[best_model_index]
#     best_observation_count = observation_counts[best_model_index]

#     return best_model_index, best_r_squared, best_observation_count