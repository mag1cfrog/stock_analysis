import duckdb
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text

def store_data_in_duckdb(data: pd.DataFrame, db_path: str, table_name: str):
    """
    Store the validated stock data in a DuckDB table.

    :param data: DataFrame containing the stock data
    :param db_path: Path to the DuckDB database file
    :param table_name: Name of the DuckDB table to store the data in
    """
    # conn = duckdb.connect(database=db_path)
    # # table_name: str = f"{symbol}_{time_unit}_{time_unit_length}"

    # # Check if the table exists, and create it if it doesn't
    # if not conn.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')").fetchone()[0]:
    #     conn.execute(f"CREATE TABLE {table_name} (timestamp TIMESTAMP, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume FLOAT, trade_count FLOAT, vwap FLOAT)")

    # # Insert new data into the table using DuckDB's pandas.to_sql function
    # conn.register('dataframe', data)
    # conn.execute(f"INSERT INTO {table_name} SELECT * FROM dataframe")
    # conn.unregister('dataframe')  # Clean up
    # conn.close()
# ---------------------------------------------------------------------------------------------
    # conn = duckdb.connect(db_path)

    # # Use the DataFrame's to_sql method with DuckDB connection, allowing schema inference
    # data.to_sql(table_name, conn, if_exists='append', index=False)

    # conn.close()
# ---------------------------------------------------------------------------------------------
    # Create a DuckDB engine
    # engine = create_engine(f"duckdb:///{db_path}")

    # with engine.connect() as conn:
    #     # Register the DataFrame with DuckDB
    #     conn.execute(text(f"REGISTER (:table_name AS data_frame)"), {"table_name": table_name, "data_frame": data})

    #     # Insert the data from the registered DataFrame into the table
    #     conn.execute(text(f"INSERT INTO {table_name} SELECT * FROM data_frame"))

    #     # Optionally, unregister the DataFrame if you don't need it anymore
    #     conn.execute(text("UNREGISTER data_frame"))
# ---------------------------------------------------------------------------------------------
    print(f"\nBelow data is passed in as the one to update: \n {data}")

    # Create a DuckDB engine using duckdb_engine
    engine = create_engine(f"duckdb:///{db_path}")

    # Use Pandas' to_sql method to insert the DataFrame into DuckDB
    # The 'if_exists' parameter controls the behavior when the table already exists
    # 'append' will insert data into the existing table or create it if it doesn't exist
    data.to_sql(table_name, engine, if_exists='append', index=False)

    with engine.connect() as conn:
        # Check if the 'timestamp' column exists in the table
        timestamp_exists = conn.execute(text(f"SELECT COUNT(*) FROM pragma_table_info('{table_name}') WHERE name='timestamp'")).scalar() > 0

        if timestamp_exists:
            # Create a new temporary table with sorted and deduplicated data
            conn.execute(text(f"""
                CREATE TEMPORARY TABLE temp_table AS
                SELECT DISTINCT * FROM {table_name}
                ORDER BY timestamp;
            """))
            
            # Use pandas.read_sql to fetch the contents of the temporary table into a DataFrame
            temp_df = pd.read_sql(text("SELECT * FROM temp_table"), conn)

            # Print the DataFrame to check its contents
            print("\nContents of temporary table 'temp_table':")
            print(temp_df)

            # Delete the original table's data
            conn.execute(text(f"DELETE FROM {table_name}"))

            # Insert sorted and deduplicated data back into the original table
            conn.execute(text(f"""
                INSERT INTO {table_name}
                SELECT * FROM temp_table;
            """))

            # Drop the temporary table
            conn.execute(text("DROP TABLE temp_table"))

    # Close the connection
    engine.dispose()
    


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
