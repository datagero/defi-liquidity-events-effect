

def replace_nulls(df):
    # Null analysis
    null_counts = df.isnull().sum()
    null_counts.sort_values(ascending=False, inplace=True)
    print(null_counts[:10])

    cols_replace_nulls = ['rate-USD-iother_01', 'avg-USD-iother_01', 'vol_0_1', 'vol_0_2', 'vol_0_3']
    for col in cols_replace_nulls:
        df[col] = df[col].fillna(0)

    null_counts = df.isnull().sum()
    null_counts.sort_values(ascending=False, inplace=True)
    print(null_counts[:10])
    print("Replaced nulls with 0s for columns: ", cols_replace_nulls)
    return df

def remove_nulls(df, explainable_variables, logs=True):
    # Remove the rows that contain null values
    initial_len = df.shape[0]
    X = df[explainable_variables]
    null_rows = X[X.isnull().any(axis=1)].index

    df = df.drop(null_rows, axis=0)
    new_len = df.shape[0]

    if logs:
        print("New row count:", df.shape[0])
        # Print difference in row count
        data_loss_percentage = ((initial_len - new_len) / initial_len) * 100
        print("Difference in row count:", initial_len - new_len)
        print('Data loss as a percentage: %.2f%%' % data_loss_percentage)
    return df

def aggregate_columns_by_interval(X, cols_aggregate_intervals_range):
    X_dropped = X.copy()
    dropped_cols = []

    for interval in cols_aggregate_intervals_range:
        cols_aggregate_intervals = [col for col in X.columns if col.startswith(interval)]
        # Sum the columns and drop the original columns
        X_dropped[interval + '03'] = X_dropped[cols_aggregate_intervals].sum(axis=1)
        X_dropped = X_dropped.drop(columns=cols_aggregate_intervals)
        dropped_cols.extend(cols_aggregate_intervals)

    return X_dropped, dropped_cols