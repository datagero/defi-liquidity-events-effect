import numpy as np

def get_size(df, size_col):
    """
    Retrieve the size from a dataframe representing a liquidity pool.

    Parameters:
        df (DataFrame): The dataframe containing liquidity pool data.
        size_col (str): The name of the column representing the size.

    Returns:
        float: The size value of the liquidity pool.

    Raises:
        AssertionError: If the size column contains more than one value.

    Notes:
        The "size" refers to the amount of liquidity or tokens present in the liquidity pool.
        It represents the total quantity of one or more tokens that participants have provided
        to enable trading in the pool.
    """

    size = df[size_col]
    if size.shape[0] == 0:
        return np.nan
    assert size.shape[0] == 1, "Size column contains more than one value"

    return size.values[0]


def get_width(df, width_col):
    """
    Retrieve the width from a dataframe representing a liquidity pool.

    Parameters:
        df (DataFrame): The dataframe containing liquidity pool data.
        width_col (str): The name of the column representing the width.

    Returns:
        float: The width value of the liquidity pool.

    Raises:
        AssertionError: If the width column contains more than one value.

    Notes:
        The "width" refers to the price range or spread in which the liquidity is provided
        in the liquidity pool. It represents the price difference between the upper and lower
        bounds of the pool's price range.
    """

    width = df[width_col]
    if width.shape[0] == 0:
        return np.nan
    assert width.shape[0] == 1, "Width column contains more than one value"
    return width.values[0]

def calculate_volatility(df, pool_price_col):
    """
    Calculate volatility based on the pool price.

    Args:
        df (DataFrame): Input DataFrame.
        pool_price_col (str): Name of the column representing the pool price.

    Returns:
        float: Volatility value.

    """
    #ignore where pool_price is nan
    df = df[df[pool_price_col].notna()]

    df['Diff'] = df[pool_price_col] -  df[pool_price_col].shift(-1)
    df = df[df['Diff'].notna()]

    df['Diff^2'] = df['Diff']**2
    volatility = np.sqrt(sum(df['Diff^2']))/len(df['Diff^2'])

    #returns = df[pool_price_col].pct_change()
    #volatility = returns.std()
    return volatility

def calculate_traded_volume_rate(df, amount_usd_col):
    """
    Calculate the rate of traded volume in USD on the WBTC-WETH pools of principal interest.
    with respect to the latest mint operations and swaps.

    Args:
        df (DataFrame): Input DataFrame filtered for the pool of interest.
        amount_usd_col (str): Name of the column representing the traded volume in USD.

    Returns:
        float: Rate of traded volume in USD on the WBTC-WETH pools of principal interest.

    """
    df_scope = df[df['transaction_type'] != 'burns']
    total_traded_volume = df_scope[amount_usd_col].sum()
    total_trading_days = df_scope['timestamp'].nunique()
    if total_trading_days == 0:
        return np.nan
    traded_volume_rate = total_traded_volume / total_trading_days
    return traded_volume_rate

def calculate_trades_count(df):
    """
    Calculate the total count of trades on the WBTC-WETH pools of principal interest.
    with respect to the latest mint operations and swaps.

    Args:
        df (DataFrame): Input DataFrame filtered for the pool of interest.

    Returns:
        int: Total count of trades on the WBTC-WETH pools of principal interest.

    """
    df_scope = df[df['transaction_type'] != 'burns']
    total_trades_count = len(df_scope)
    return total_trades_count

def calculate_average_volume(df, amount_usd_col):
    """
    Calculate the average traded volume in USD on the WBTC-WETH pools of principal interest.
    with respect to the latest mint operations and swaps.

    Args:
        df (DataFrame): Input DataFrame filtered for the pool of interest.
        amount_col (str): Name of the column representing the traded volume.

    Returns:
        float: Average traded volume in USD on the WBTC-WETH pools of principal interest.

    """
    df_scope = df[df['transaction_type'] != 'burns']
    average_volume = df_scope[amount_usd_col].mean()
    return average_volume


def calculate_tvl_difference_ratio(df, pool_col, tvl_col):
    """
    NOTE -> Still in development, currently not used in the project.
    Calculate the difference and ratio of the latest TVL in pool 3000 and pool 500.

    Args:
        df (DataFrame): Input DataFrame containing TVL data for the pools.
        pool_col (str): Name of the column representing the pool.
        tvl_col (str): Name of the column representing the TVL.

    Returns:
        tuple: A tuple containing the difference and ratio of the latest TVL in pool 3000 and pool 500.

    """
    pool_3000_tvl = df.loc[df[pool_col] == 3000, tvl_col].iloc[-1]
    pool_500_tvl = df.loc[df[pool_col] == 500, tvl_col].iloc[-1]
    tvl_difference = pool_3000_tvl - pool_500_tvl
    tvl_ratio = pool_3000_tvl / pool_500_tvl
    return tvl_difference, tvl_ratio
