





def backtest_strategy_short_long_window(args, data, trading_strategy_func, cash=10000, shares_held = 0):
    """
    Backtests a trading strategy and returns the results.
    
    Parameters
    ----------
    args : tuple
        Tuple containing the short and long window sizes
        data : DataFrame
        DataFrame containing the stock price data
    """
    short_window, long_window = args
    simulation_df = trading_strategy_func(data, short_window, long_window)
    normalization_factor = cash / data['close'][0]
    
    return simulation_df