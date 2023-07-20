import pandas as pd

# Read data
df_base = pd.read_csv("Data/processed/features/df_features_raw_ref500.csv")
df_500 = pd.read_csv("Data/processed/features/df_features_raw_ref500.csv")
df_3000 = pd.read_csv("Data/processed/features/df_features_raw_ref3000.csv")


df_30horizons = df_base[df_base['horizon_label'] <= 30]

pass