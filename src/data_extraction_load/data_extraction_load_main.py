import argparse
import os
import sys
from pathlib import Path
from datetime import datetime

import duckdb

# Calculate the path to the root of the repository
root_path = Path(__file__).parent.parent.parent
sys.path.append(str(root_path))

from src.data_extraction_load.fetch_data import fetch_stock_data
from src.data_extraction_load.store_data import store_data_in_duckdb
from src.data_extraction_load.validate_data import validate_data
from src.data_extraction_load.update_data import update_stock_data_wrapper, get_last_timestamp
from src.common.log_config import setup_logging


# Configure logging at the start
setup_logging()


def table_exists(conn, table_name):
    return conn.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')").fetchone()[0]


def is_table_empty(conn, table_name):
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] == 0


def main(symbol, time_unit, time_unit_length):
    # Define the database file path
    # Construct the path to the database file
    db_path = root_path / 'data' / 'raw_stock_price' / 'stock_bar_data.db'
    db_path_str = str(db_path)
    print("Attempting to connect to DB at:", db_path)
    # Connect to the DuckDB database
    conn = duckdb.connect(db_path_str)
    table_name = f"{symbol}_{time_unit}_{time_unit_length}_since2022"

    # Check if the table exists and whether it has data
    if not table_exists(conn, table_name) or is_table_empty(conn, table_name):
        # Perform an initial load
        start_date = datetime(2022, 1, 1)  # Use your actual desired start date
        end_date = datetime.now()
        data = fetch_stock_data(symbol, start_date, end_date, time_unit, time_unit_length)
        if not data.empty:
            validated_data = validate_data(data)
            store_data_in_duckdb(validated_data, db_path_str, table_name)
    else:
        # Perform an update
        update_stock_data_wrapper(symbol, time_unit, time_unit_length, db_path_str, table_name)
    
    # Close the database connection
    conn.close()
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Stock Data Extraction and Loading')
    parser.add_argument('symbol', type=str, help='Stock symbol')
    parser.add_argument('time_unit', type=str, help='Time unit for the data (minute, hour, day)')
    parser.add_argument('time_unit_length', type=int, help='Length of each time unit in minutes')
    args = parser.parse_args()

    main(args.symbol, args.time_unit, args.time_unit_length)
