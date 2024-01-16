import polars as pl
import duckdb






# Core function where the trading strategy is implemented
def default_gold_cross_death_cross(
    data, short_window, long_window, default_percentage_of_cash_invested=0
):
    """
    Simulates trading based on the signals in the DataFrame

    Parameters
    ----------
    data : DataFrame
        DataFrame containing the signals

    Returns
    -------
    simulation_df : DataFrame
        DataFrame containing the simulation results


    """

    # simulation_results = []
    # normalization_factor = cash / data['close'][0]

    data = data.with_columns(
        data["close"].rolling_mean(window_size=int(short_window)).alias("short_MA"),
        data["close"].rolling_mean(window_size=int(long_window)).alias("long_MA"),
    )

    # Create conditions for buying and selling
    buy_condition = (data["short_MA"] > data["long_MA"]) & (
        data["short_MA"].shift(1) <= data["long_MA"].shift(1)
    )
    sell_condition = (data["short_MA"] < data["long_MA"]) & (
        data["short_MA"].shift(1) >= data["long_MA"].shift(1)
    )

    data = data.with_columns(
        (
            pl.when(buy_condition).then(1).otherwise(0)
            + default_percentage_of_cash_invested
        )
        .cum_sum()
        .alias("cumulative_buy"),
        pl.when(sell_condition).then(1).otherwise(0).cum_sum().alias("cumulative_sell"),
    )

    data = data.with_columns(
        (data["cumulative_buy"] - data["cumulative_sell"]).alias(
            "percentage_of_cash_invested"
        )
    )
    # Amend sell points where there is no stock to sell
    should_not_sell_because_donot_have = min(data["percentage_of_cash_invested"])
    data = data.with_columns(
        (pl.col("cumulative_sell") + should_not_sell_because_donot_have).alias(
            "cumulative_sell"
        )
    )
    data = data.with_columns(
        pl.when(pl.col("cumulative_sell") < 0)
        .then(0)
        .otherwise(pl.col("cumulative_sell"))
        .alias("cumulative_sell")
    )
    data = data.with_columns(
        (data["cumulative_buy"] - data["cumulative_sell"]).alias(
            "percentage_of_cash_invested"
        )
    )

    return data.drop('short_MA', 'long_MA', 'cumulative_buy', 'cumulative_sell')
    # return data














def main():
    # Time the execution
    import time

    # start_time = time.time()
    # short_range = range(1, 100)
    # long_range = range(1, 100)
    # db_path = r"E:\git_repos\stock_analysis\data\raw_stock_price\stock_bar_data.db"
    # conn = duckdb.connect(db_path)
    # query = """
    # SELECT * FROM NVDA_minute_30
    # """
    # stock_df = conn.execute(query).pl().sort("timestamp")
    # conn.close()

    # results_df = sensitivity_analysis(stock_df, short_range, long_range)
    # print(results_df)
    # analyze_and_visualize(results_df)
    # print(f"Time taken: {time.time() - start_time} seconds")
    # run_dash_app(results_df.to_pandas().set_index("Short_Window"))


if __name__ == "__main__":
    main()
