import polars as pl
import duckdb


def calculate_moving_averages(data, short_window, long_window):
    data = data.with_columns(
        data['close'].rolling_mean(window_size=int(short_window)).alias('Short_MA'),
        data['close'].rolling_mean(window_size=int(long_window)).alias('Long_MA')
    )
    return data




def main():
    db_path = r'E:\git_repos\stock_analysis\data\raw_stock_price\stock_bar_data.db'
    conn = duckdb.connect(db_path)
    query = """
    SELECT * FROM NVDA_day_1
    """
    df = conn.execute(query).pl()
    df = calculate_moving_averages(df, 20, 50)
    print(df)

if __name__ == '__main__':
    main()