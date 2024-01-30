import duckdb
import pandas as pd


def store_data_in_duckdb(data: pd.DataFrame, symbol: str, time_unit: str, time_unit_length: int, db_path: str):
    """
    Store the validated stock data in a DuckDB table.

    :param data: DataFrame containing the stock data
    :param symbol: Stock symbol, e.g., 'AAPL'
    :param time_unit: Time unit for the data, e.g., 'minute'
    :param time_unit_length: Length of each time unit, e.g., 30
    :param db_path: Path to the DuckDB database file
    """
    conn = duckdb.connect(database=db_path)
    table_name = f"{symbol}_{time_unit}_{time_unit_length}"

    # Check if the table exists, and create it if it doesn't
    if not conn.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')").fetchone()[0]:
        conn.execute(f"CREATE TABLE {table_name} (timestamp TIMESTAMP, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume FLOAT, trade_count FLOAT, vwap FLOAT)")

    # Insert new data into the table using DuckDB's pandas.to_sql function
    conn.register('dataframe', data)
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM dataframe")
    conn.unregister('dataframe')  # Clean up
    conn.close()


def main():
    """
    Main function for testing the store_data_in_duckdb function.
    """
    # Example data for testing
    data = {
        'timestamp': pd.to_datetime(['2022-01-03 09:00:00', '2022-01-03 09:30:00']),
        'open': [176.01, 175.62],
        'high': [176.08, 175.85],
        'low': [175.51, 175.51],
        'close': [175.57, 175.83],
        'volume': [30884.0, 30053.0],
        'trade_count': [1008.0, 719.0],
        'vwap': [175.768446, 175.635803]
    }
    df = pd.DataFrame(data)

    # Replace the parameters with your actual data and database path
    symbol = "AAPL"
    time_unit = "minute"
    time_unit_length = 30
    db_path = ":memory:"  # Use an actual file path to persist data

    store_data_in_duckdb(df, symbol, time_unit, time_unit_length, db_path)
    print("Data storage successful.")


if __name__ == "__main__":
    main()
