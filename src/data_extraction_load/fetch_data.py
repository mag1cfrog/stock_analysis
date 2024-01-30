import os
import pandas as pd
from datetime import datetime
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit


def set_time_frame(time_unit_length: int, time_unit: str) -> TimeFrame:
    """
    Set the time frame for the data to be extracted.
    """
    time_unit = time_unit.lower()
    if time_unit in ["minute", "min"]:
        return TimeFrame(time_unit_length, TimeFrameUnit.Minute)
    elif time_unit in ["hour", "hr"]:
        return TimeFrame(time_unit_length, TimeFrameUnit.Hour)
    elif time_unit == "day":
        return TimeFrame(time_unit_length, TimeFrameUnit.Day)
    else:
        raise ValueError(f"Unsupported time unit: {time_unit}")


def fetch_stock_data(symbol: str, start_date: datetime, end_date: datetime, time_unit: str, time_unit_length: int = 1, adjustment: str = "all") -> pd.DataFrame:
    """
    Fetch stock bar data from Alpaca API.
    """
    client = StockHistoricalDataClient(
        api_key=os.getenv("ALPACA_API_KEY"),
        secret_key=os.getenv("ALPACA_SECRET_KEY")
    )

    timeframe = set_time_frame(time_unit_length, time_unit)

    request_params = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=timeframe,
        start=start_date,
        end=end_date,
        adjustment=adjustment
    )

    stock_bar = client.get_stock_bars(request_params)
    data_df = stock_bar.df if stock_bar.data else pd.DataFrame()
    return data_df.reset_index() if not data_df.empty else data_df


def main():
    """
    Main function for testing the fetch_stock_data function.
    """
    symbol = "AAPL"  # Example symbol
    start_date = datetime(2022, 1, 1)  # Example start date
    end_date = datetime.now()  # Current time as end date
    time_unit = "minute"  # Example time unit
    time_unit_length = 30  # Example time unit length

    data = fetch_stock_data(symbol, start_date, end_date, time_unit, time_unit_length)
    print(data.head())  # Display the first few rows of the fetched data


if __name__ == "__main__":
    main()
