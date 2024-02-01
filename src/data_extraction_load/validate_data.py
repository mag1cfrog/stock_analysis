from datetime import datetime
import pandas as pd
import sys
from pathlib import Path

# Calculate the path to the root of the repository
root_path = Path(__file__).parent.parent.parent
sys.path.append(str(root_path))

from src.data_extraction_load.fetch_data import fetch_stock_data



def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the DataFrame containing stock data.

    :param df: DataFrame to validate
    :return: Validated DataFrame
    :raises: ValueError if any validation checks fail
    """
    # Check for missing values
    if df.isnull().any().any():
        raise ValueError("Data contains missing values.")

    # Validate data types
    expected_dtypes = {
        'symbol': 'object',
        'timestamp': 'datetime64[ns, UTC]',
        'open': 'float',
        'high': 'float',
        'low': 'float',
        'close': 'float',
        'volume': 'float',
        'trade_count': 'float',
        'vwap': 'float'
    }
    for column, expected_dtype in expected_dtypes.items():
        if df[column].dtype != expected_dtype:
            raise ValueError(f"Incorrect data type for column {column}. Expected: {expected_dtype}, Found: {df[column].dtype}")

    # Validate numeric values are non-negative
    numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'trade_count']
    if (df[numeric_columns] < 0).any().any():
        raise ValueError("Numeric columns contain negative values.")

    return df


def main():
    """
    Main function for testing the validate_data function using real fetched data.
    """
    symbol = "AAPL"  # Example symbol
    start_date = datetime(2022, 1, 1)  # Example start date
    end_date = datetime.now()  # Example end date
    time_unit = "minute"  # Example time unit
    time_unit_length = 30  # Example time unit length

    # Fetch data using the fetch_stock_data function from the fetch_data module
    try:
        df = fetch_stock_data(symbol, start_date, end_date, time_unit, time_unit_length)
        print("Fetched data:")
        print(df.head())  # Display the first few rows of the fetched data

        # Validate the fetched data
        validated_df = validate_data(df)
        print("Data validation successful.")
        print(validated_df.head())  # Display the first few rows of the validated data
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()