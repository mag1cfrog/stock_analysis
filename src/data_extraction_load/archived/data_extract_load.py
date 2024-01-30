import os
from datetime import datetime, timedelta, date
import duckdb
import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
import pandas_market_calendars as mcal
import pytz


def set_time_frame(time_unit_length: int, time_unit: str):
    """
    Set the time frame for the data to be extracted
    :param time_unit_length: The length of the time unit
    :param time_unit: The time unit, choose from 'minute', 'hour', 'day'
    :return: TimeFrame object
    """
    if time_unit.lower() in ["minute", "min"]:
        return TimeFrame(time_unit_length, TimeFrameUnit.Minute)
    elif time_unit.lower() in ["hour", "hr"]:
        return TimeFrame(time_unit_length, TimeFrameUnit.Hour)
    elif time_unit.lower() == "day":
        return TimeFrame(time_unit_length, TimeFrameUnit.Day)


def stock_bar_data_download(
    symbol: str,
    Start_Date: datetime,
    End_Date: datetime,
    time_unit: str,
    time_unit_length: int = 1,
    adjustment: str = "all",
):
    """
    Download stock bar data from Alpaca API
    :param symbol: The stock symbol
    :param Start_Date: The start date of the data
    :param End_Date: The end date of the data
    :param time_unit: The time unit, choose from 'minute', 'hour', 'day'
    :param time_unit_length: The length of the time unit
    :param adjustment: The adjustment method, choose from 'all', 'split', 'dividend'
    :return: A pandas dataframe
    """
    # Initialize client with api key and secret key
    client = StockHistoricalDataClient(
        api_key=os.getenv("ALPACA_API_KEY"), secret_key=os.getenv("ALPACA_SECRET_KEY")
    )
    # set the time frame
    timeframe = set_time_frame(time_unit_length, time_unit)
    # Request data for a single stock
    request_params = StockBarsRequest(
        symbol_or_symbols=[symbol],
        timeframe=timeframe,
        start=Start_Date,
        end=End_Date,
        adjustment=adjustment,
    )
    # Get the data and return a pandas dataframe
    stock_bar = client.get_stock_bars(request_params)
    data_df = stock_bar.df if stock_bar.data else pd.DataFrame()
    return data_df.reset_index() if not data_df.empty else data_df


def update_and_retrieve_stock_data(
    symbol: str,
    start_date: datetime,
    end_date: datetime,
    time_unit: str,
    db_path: str,
    result_type: str,
    time_unit_length: int = 1,
    adjustment: str = "all",
):
    # Connect to DuckDB
    conn = duckdb.connect(db_path)

    # Name of the table (you might want to use a consistent naming convention)
    table_name = f"{symbol}_{time_unit}_{time_unit_length}"

    # Query to select data within the desired period
    query = f"SELECT * FROM {table_name} WHERE timestamp >= '{start_date}' AND timestamp <= '{end_date}'"

    # Check if the table exists and has the desired data
    if (
        conn.execute(
            f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchone()[0]
        > 0
    ):
        # Check if data for the desired period exists
        query = f"SELECT * FROM {table_name} WHERE timestamp >= '{start_date}' AND timestamp <= '{end_date}'"
        existing_data = conn.execute(query).df()
        # Convert existing_data['timestamp'] to naive
        existing_data["timestamp"] = existing_data["timestamp"].dt.tz_localize(None)

        # If data for the entire period exists
        if (
            not existing_data.empty
            and existing_data["timestamp"].min() <= start_date
            and existing_data["timestamp"].max() >= end_date
        ):
            conn.close()
            return existing_data

        # Identify missing periods
        missing_periods = identify_missing_periods(
            existing_data, start_date, end_date, time_unit, time_unit_length
        )

        # print(f"Missing periods: {(missing_periods)}")
        # Fetch missing data
        # Initialize an empty list to collect DataFrames
        new_data_list = []
        for period in missing_periods:
            # Fetch new data for each missing period and add it to the list
            new_data_list.append(
                stock_bar_data_download(
                    symbol,
                    period[0],
                    period[1],
                    time_unit,
                    time_unit_length,
                    adjustment,
                )
            )

        # Concatenate all DataFrames in the list
        new_data = pd.concat(new_data_list)
        # Append new data to the table and sort
        if not new_data.empty:
            new_data.to_sql(table_name, conn, if_exists="append", index=False)
            conn.execute(
                f"DELETE FROM {table_name} WHERE timestamp IN (SELECT timestamp FROM {table_name} GROUP BY timestamp HAVING count(*) > 1)"
            )  # Remove duplicates
            conn.commit()

    else:
        # If table doesn't exist, fetch all data and create the table
        new_data = stock_bar_data_download(
            symbol, start_date, end_date, time_unit, time_unit_length, adjustment
        )
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM new_data")
        conn.commit()

    # Retrieve the desired data
    final_data = conn.execute(query)
    if result_type == "pandas":
        final_data = final_data.df()
    elif result_type == "polars":
        final_data = final_data.pl()
    else:
        raise ValueError("Invalid result type. Choose from 'pandas', 'polars'.")
    conn.close()
    return final_data


def is_trading_time(datetime_to_check, calendar):
    """
    Check if a given datetime is during NYSE trading hours.
    """
    eastern = pytz.timezone('America/New_York')
    datetime_to_check = datetime_to_check.replace(tzinfo=pytz.utc).astimezone(eastern)
    # Get trading sessions for the date
    trading_sessions = calendar.schedule(start_date=datetime_to_check.date(), end_date=datetime_to_check.date())

    if trading_sessions.empty:
        return False  # No trading sessions on this day

    # Check if datetime_to_check falls within any trading session
    for _, session in trading_sessions.iterrows():
        if session['market_open'] <= datetime_to_check <= session['market_close']:
            return True

    return False


def identify_missing_periods(
    existing_data, start_date, end_date, time_unit, time_unit_length=1
):
    # Get the market calendar
    nyse_calendar = mcal.get_calendar("NYSE")
    # Pre-generate trading sessions
    trading_sessions = nyse_calendar.schedule(start_date=start_date, end_date=end_date)

    if time_unit.lower() in ["minute", "min"]:
        delta = pd.Timedelta(minutes=time_unit_length)
    elif time_unit.lower() in ["hour", "hr"]:
        delta = pd.Timedelta(hours=time_unit_length)
    elif time_unit.lower() == "day":
        delta = pd.Timedelta(days=time_unit_length)
    else:
        raise ValueError("Invalid time unit. Choose from 'minute', 'hour', 'day'.")

    missing_periods = []
    current_period_start = start_date
    current_missing_start = None
    trading_sessions['market_open'] = trading_sessions['market_open'].dt.tz_localize(None)
    trading_sessions['market_close'] = trading_sessions['market_close'].dt.tz_localize(None)

    while current_period_start < end_date:
        current_period_end = current_period_start + delta

        # Check if the period overlaps with any trading session
        if any((session.market_open <= current_period_start) and (session.market_close >= current_period_end) for _, session in trading_sessions.iterrows()):
        # if True: # Assuming always a trading day for simplicity
            # Check if there is data for the current period
            if existing_data[
                (existing_data["timestamp"] >= current_period_start)
                & (existing_data["timestamp"] < current_period_end)
            ].empty:
                if current_missing_start is None:
                    current_missing_start = current_period_start
                # Extend the missing period
                current_missing_end = current_period_end
            else:
                # Append the missing period if any and reset
                if current_missing_start is not None:
                    missing_periods.append((current_missing_start, current_missing_end))
                    current_missing_start = None

        current_period_start = current_period_end

     # Append the last missing period if it ends at the end_date
    if current_missing_start is not None:
        missing_periods.append((current_missing_start, current_missing_end))

    print(f"Missing periods: {missing_periods}")
    
    return missing_periods


def main():
    db_path = r"E:\git_repos\stock_analysis\data\raw_stock_price\stock_bar_data.db"
    df = update_and_retrieve_stock_data(
        "NVDA",
        datetime.today() - timedelta(days=180),
        datetime.today(),
        "minute",
        db_path,
        result_type="polars",
        time_unit_length=30,
    )
    print(df)


if __name__ == "__main__":
    main()
