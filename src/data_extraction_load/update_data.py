import sys
from pathlib import Path
from datetime import datetime, timedelta
import logging
import duckdb
import pandas as pd

# Calculate the path to the root of the repository
root_path = Path(__file__).parent.parent.parent
sys.path.append(str(root_path))

from src.common.notifications import send_email
from src.common.log_config import setup_logging
from fetch_data import fetch_stock_data
from validate_data import validate_data
from store_data import store_data_in_duckdb

setup_logging()


def get_last_timestamp(db_path: str, table_name: str) -> datetime:
    """
    Retrieves the last timestamp from the specified DuckDB table.

    :param db_path: Path to the DuckDB database file.
    :param table_name: Name of the table to query.
    :return: The last timestamp in the table.
    """
    conn = duckdb.connect(db_path)
    result = conn.execute(f"SELECT MAX(timestamp) FROM {table_name}").fetchone()
    conn.close()
    return result[0] if result[0] is not None else datetime(2022, 1, 1)  # Default to Jan 1, 2022, if no data


def update_stock_data(symbol: str, time_unit: str, time_unit_length: int, db_path: str):
    """
    Updates the stock data in DuckDB with new data since the last timestamp.

    :param symbol: Stock symbol, e.g., 'NVDA'.
    :param time_unit: Time unit for the data, e.g., 'minute'.
    :param time_unit_length: Length of each time unit, e.g., 30.
    :param db_path: Path to the DuckDB database file.
    """
    table_name = f"{symbol}_{time_unit}_{time_unit_length}"
    last_timestamp = get_last_timestamp(db_path, table_name)

    # Fetch new data starting just after the last available timestamp
    new_data = fetch_stock_data(symbol, last_timestamp + timedelta(minutes=1), datetime.now(), time_unit, time_unit_length)

    if not new_data.empty:
        # Validate and preprocess the new data
        validated_data = validate_data(new_data)

        # Store the new data in DuckDB
        store_data_in_duckdb(validated_data, symbol, time_unit, time_unit_length, db_path)


def update_stock_data_wrapper(*args, **kwargs):
    try:
        update_stock_data(*args, **kwargs)
    except Exception as e:
        # Record the error in the log file
        logging.error(f"Failed to update stock data: {e}", exc_info=True)

        # Send an email notification to the administrator
        subject = "Stock Data Update Error"
        body = f"An error occurred while updating stock data: {e}"
        to_addr = "mag1cfrogginger@gmail.com"  
        from_addr = "harrywong2017@gmail.com"
        # temporary comment out send_email(subject, body, to_addr, from_addr) during testing
        # send_email(subject, body, to_addr, from_addr)



def main():
    """
    Main function to test the update_stock_data function.
    """
    # Update the parameters according to your setup
    symbol = "NVDA"
    time_unit = "minute"
    time_unit_length = 30
    db_path = "path_to_your_duckdb_file.duckdb"  # Replace with your actual DuckDB file path

    update_stock_data_wrapper(symbol, time_unit, time_unit_length, db_path)
    print("Stock data update process completed successfully.")


if __name__ == "__main__":
    main()
