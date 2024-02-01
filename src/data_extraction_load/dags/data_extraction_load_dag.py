import subprocess
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
from airflow import DAG
from airflow.operators.python import PythonOperator
import pytz

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,  # Number of retries
    'retry_delay': timedelta(minutes=2),  # Delay between retries
    'email': ['mag1cfrogginger@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # Add any other necessary default arguments
}


# Initialize the DAG
dag = DAG(
    'data_extraction_load_main',
    default_args=default_args,
    description='Runs data_extraction_load_main.py every half hour during NYSE trading hours',
    schedule_interval='*/30 * * * *',  # This is a cron expression for every half hour
)


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


# Define the Python function to execute
def conditionally_run_data_extraction(**context):
    datetime_to_check = context['execution_date']
    nyse_calendar = mcal.get_calendar("NYSE")

    # Your function to check if it's trading time, returning True or False
    if is_trading_time(datetime_to_check, nyse_calendar):
        # Call the data_extraction_load_main.py with the required parameters
        subprocess.run(['python', 'git_repos/stock_analysis/src/data_extraction_load/data_extraction_load_main.py', 'NVDA', 'minute', '30'], check=True)


# Create the PythonOperator
run_data_extraction = PythonOperator(
    task_id='run_data_extraction_if_trading_time',
    python_callable=conditionally_run_data_extraction,
    provide_context=True,
    dag=dag,
)

