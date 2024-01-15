import duckdb
from strategy_constructor.default_gold_cross_death_cross import default_gold_cross_death_cross




def main():
    # Time the execution
    import time
    start_time = time.time()
    short_range = range(1, 100)
    long_range = range(1, 100)
    db_path = r'E:\git_repos\stock_analysis\data\raw_stock_price\stock_bar_data.db'
    conn = duckdb.connect(db_path)
    query = """
    SELECT * FROM NVDA_minute_30
    """
    stock_df = conn.execute(query).pl().sort('timestamp')
    conn.close()
        
    results_df = default_gold_cross_death_cross(stock_df, 10, 16)
    print(results_df)    
    # results_df = sensitivity_analysis(stock_df, short_range, long_range)
    # print(results_df)
    # analyze_and_visualize(results_df)
    print(f"Time taken: {time.time() - start_time} seconds")
    # run_dash_app(results_df.to_pandas().set_index('Short_Window'))


if __name__ == '__main__':
    main()
