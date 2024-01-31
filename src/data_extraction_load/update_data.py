import sys
from pathlib import Path
from datetime import datetime, timedelta
import logging
import pytz
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


def update_stock_data(symbol: str, time_unit: str, time_unit_length: int, db_path: str, table_name: str):
    last_timestamp = get_last_timestamp(db_path, table_name)
    last_timestamp_utc = last_timestamp.astimezone(pytz.timezone('utc'))

    # Make datetime.now() timezone-aware by setting it to american central time
    current_utc_time_15mindelay = datetime.now(tz=pytz.utc) - timedelta(minutes=16)

    if time_unit == "minute":
        minimum_timedelta = timedelta(minutes=time_unit_length)
    elif time_unit == "hour":
        minimum_timedelta = timedelta(hours=time_unit_length)
    elif time_unit == "day":
        minimum_timedelta = timedelta(days=time_unit_length)

    if current_utc_time_15mindelay - last_timestamp_utc >= minimum_timedelta:
        print("\ncurrent utc time: ", current_utc_time_15mindelay, "\nlast timestamp utc: ", last_timestamp_utc, "\ntime difference: ", current_utc_time_15mindelay - last_timestamp_utc)

        new_data = fetch_stock_data(symbol, last_timestamp_utc + minimum_timedelta, current_utc_time_15mindelay, time_unit, time_unit_length)

    if not new_data.empty:
        validated_data = validate_data(new_data)
        store_data_in_duckdb(validated_data, db_path, table_name)
        
    else:
        print("No new data to update.")

    



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
