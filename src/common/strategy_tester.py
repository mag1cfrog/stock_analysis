import polars as pl
from concurrent.futures import ProcessPoolExecutor
import itertools


def backtest_strategy_short_long_window(
    args, data, trading_strategy_func, cash=100000, shares_held=0
):
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
    normalization_factor = cash / data["close"][0]
    simulation_results = []
    for i in range(simulation_df.height):
        if i >= 1:
            # Buy condition
            if (
                simulation_df["percentage_of_cash_invested"][i]
                > simulation_df["percentage_of_cash_invested"][i - 1]
            ):
                shares_to_buy = (
                    cash
                    * (
                        simulation_df["percentage_of_cash_invested"][i]
                        - simulation_df["percentage_of_cash_invested"][i - 1]
                    )
                    // data["close"][i]
                )
                cash -= shares_to_buy * data["close"][i]
                shares_held += shares_to_buy
            # Sell condition
            elif (
                simulation_df["percentage_of_cash_invested"][i]
                < simulation_df["percentage_of_cash_invested"][i - 1]
            ):
                shares_to_sell = shares_held * (
                    simulation_df["percentage_of_cash_invested"][i - 1]
                    - simulation_df["percentage_of_cash_invested"][i]
                )
                cash += shares_to_sell * data["close"][i]
                shares_held -= shares_to_sell
        asset_value = cash + shares_held * data["close"][i]
        simulation_results.append(
            {
                "timestamp": data["timestamp"][i],
                "asset_value": asset_value,
                "cash": cash,
                "shares_held": shares_held,
                "buy_point": simulation_df["percentage_of_cash_invested"][i]
                > simulation_df["percentage_of_cash_invested"][i - 1],
                "sell_point": simulation_df["percentage_of_cash_invested"][i]
                < simulation_df["percentage_of_cash_invested"][i - 1],
                "percentage_of_cash_invested": simulation_df[
                    "percentage_of_cash_invested"
                ][i],
                "normalized_stock_price": data["close"][i] * normalization_factor,
            }
        )
    simulation_df = pl.DataFrame(simulation_results)
    return simulation_df


# Function to compute both the entire backtest and the final asset value for a combination
def compute_backtest_and_final_value(combination, data_df, trading_strategy_func):
    short_window, long_window = combination
    # Get the full backtest result
    full_backtest_result = backtest_strategy_short_long_window((short_window, long_window), data_df, trading_strategy_func)
    # Extract the final asset value
    final_asset_value = full_backtest_result["asset_value"][-1]
    return full_backtest_result, final_asset_value


def backtest_on_range(data_df, short_range, long_range, trading_strategy_func):
    # Prepare the combinations of short and long windows
    combinations = list(itertools.product(short_range, long_range))

    # Use ProcessPoolExecutor to parallelize the computation
    with ProcessPoolExecutor() as executor:
        backtest_and_values = list(
            executor.map(
                compute_backtest_and_final_value, combinations, [data_df] * len(combinations), [trading_strategy_func] * len(combinations)
            )
        )

    # Create a dictionary to store the complete backtest results
    complete_backtest_results = {}
    final_asset_values = []
    for ((short, long), (full_backtest_result, final_value)) in zip(combinations, backtest_and_values):
        complete_backtest_results[(short, long)] = full_backtest_result.to_pandas()
        final_asset_values.append(final_value)
    
    # Create the results DataFrame for final asset values
    results = [
        {"short_window": short, "long_window": long, "final_asset_value": value}
        for ((short, long), value) in zip(combinations, final_asset_values)
    ]
    final_values_df = pl.DataFrame(results)

    # Reshape the DataFrame for final asset values
    reshaped_final_values_df = final_values_df.pivot(
        index="short_window", columns="long_window", values="final_asset_value"
    )

    # Now you have a dictionary where you can easily access the full backtest result
    # by using the short and long window as a key
    # reshaped_final_values_df - a DataFrame of final asset values

    return reshaped_final_values_df, complete_backtest_results