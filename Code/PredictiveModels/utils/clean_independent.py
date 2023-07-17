def print_nulls(df, title):
    """Print the number of nulls in the dataframe."""
    null_counts = df.isnull().sum()
    null_counts.sort_values(ascending=False, inplace=True)
    print(f"\n{title}\n", null_counts[:10])


def replace_nulls(df, cols_replace_nulls):
    """
    Replace nulls in specified columns with 0.

    Parameters
    ----------
    df : DataFrame
        DataFrame to replace null values in.

    Returns
    -------
    DataFrame
        DataFrame after replacing nulls with 0.
    """
    print_nulls(df, "Null Counts Before Replacement")

    df[cols_replace_nulls] = df[cols_replace_nulls].fillna(0)

    print_nulls(df, "Null Counts After Replacement")
    print("Replaced nulls with 0s for columns: ", cols_replace_nulls)

    return df


def print_row_removal_logs(initial_len, new_len):
    """Print the logs related to row removal."""
    print("New row count:", new_len)
    data_loss_percentage = ((initial_len - new_len) / initial_len) * 100
    print("Difference in row count:", initial_len - new_len)
    print('Data loss as a percentage: %.2f%%' % data_loss_percentage)


def remove_nulls(df, explainable_variables, logs=True):
    """
    Remove rows containing nulls.

    Parameters
    ----------
    df : DataFrame
        DataFrame to remove null values from.
    explainable_variables : list
        List of variables to consider for null value removal.
    logs : bool, optional
        Flag indicating whether to print logs related to row removal.

    Returns
    -------
    DataFrame
        DataFrame after removing rows with null values.
    """
    initial_len = df.shape[0]
    df = df.drop(df[df[explainable_variables].isnull().any(axis=1)].index)

    if logs:
        print_row_removal_logs(initial_len, df.shape[0])

    return df


def aggregate_columns_by_interval(df, cols_aggregate_intervals_range):
    """
    Aggregate columns by specified intervals.

    Parameters
    ----------
    df : DataFrame
        DataFrame to aggregate columns of.
    cols_aggregate_intervals_range : list
        List of column intervals to aggregate.

    Returns
    -------
    DataFrame
        DataFrame after aggregating columns.
    list
        List of dropped columns after aggregation.
    """
    dropped_cols = []
    for interval in cols_aggregate_intervals_range:
        cols_aggregate_intervals = [col for col in df.columns if col.startswith(interval)]
        df[interval + '03'] = df[cols_aggregate_intervals].sum(axis=1)
        df = df.drop(columns=cols_aggregate_intervals)
        dropped_cols.extend(cols_aggregate_intervals)

    return df, dropped_cols
