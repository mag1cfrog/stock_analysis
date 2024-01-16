import time
import duckdb
from strategy_constructor.default_gold_cross_death_cross import (
    default_gold_cross_death_cross,
)
from common.strategy_tester import sensitivity_analysis
from common.visulizer import heatmap_visualizer_with_slider_on_dash
from pytz import timezone


def main():
    start_time = time.time()
    db_path = r'E:\git_repos\stock_analysis\data\raw_stock_price\stock_bar_data.db'
    conn = duckdb.connect(db_path)
    query = """
    SELECT * FROM NVDA_minute_30
    """
    stock_df = conn.execute(query).pl().sort('timestamp')
    conn.close()
    short_range = range(1, 100)
    long_range = range(1, 100)   
    # results_df = default_gold_cross_death_cross(stock_df, 10, 16)
    results_df = sensitivity_analysis(stock_df, short_range, long_range, default_gold_cross_death_cross)
    # print(results_df.head(50))    
    central_timezone = timezone('America/Chicago')
    print(f'Time taken: {time.time() - start_time:.2f} seconds')
    heatmap_visualizer_with_slider_on_dash(results_df)
    


if __name__ == '__main__':
    main()
