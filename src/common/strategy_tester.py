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


# Function to compute final asset value for a combination
def compute_final_asset_value(combination, data_df, trading_strategy_func):
    short_window, long_window = combination
    return backtest_strategy_short_long_window((short_window, long_window), data_df, trading_strategy_func)[
        "asset_value"
    ][
        -1
    ]  # Return the final asset value


def sensitivity_analysis(data_df, short_range, long_range, trading_strategy_func):
    # Prepare the combinations of short and long windows
    combinations = list(itertools.product(short_range, long_range))

    # Use ThreadPoolExecutor to parallelize the computation
    with ProcessPoolExecutor() as executor:
        final_asset_values = list(
            executor.map(
                compute_final_asset_value, combinations, [data_df] * len(combinations), [trading_strategy_func] * len(combinations)
            )
        )

    # Create the results DataFrame
    results = [
        {"short_window": short, "long_window": long, "final_asset_value": value}
        for ((short, long), value) in zip(combinations, final_asset_values)
    ]
    results_df = pl.DataFrame(results)

    # Reshape the DataFrame
    results_df = results_df.pivot(
        index="short_window", columns="long_window", values="final_asset_value"
    )

    return results_df