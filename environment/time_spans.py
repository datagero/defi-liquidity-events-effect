import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

def load_run_config(env_file):
    """Load environment variables from a given file."""
    if not load_dotenv(env_file):
        raise Exception(f"Failed to load environment file: {env_file}")

    time_span_key = os.getenv("TIME_SPAN")
    if time_span_key is None:
        raise Exception("TIME_SPAN not found in the environment variables.")
    return time_span_key

def unix_to_datetime(unix_timestamp):
    """Converts UNIX timestamp to datetime object."""
    return datetime.utcfromtimestamp(unix_timestamp)

def generate_date_scope(start_unix, end_unix):
    """Generates a list of dates in YYYY-MM-DD format between two UNIX timestamps."""
    start_date = unix_to_datetime(start_unix)
    end_date = unix_to_datetime(end_unix)
    date_list = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((end_date - start_date).days + 1)]
    return date_list


time_spans = {
    "DEMO": {
        "start": 1655593200,  # Sat Jun 18 2022 23:00:00 GMT+0000
        "end": 1658185200     # Mon Jul 18 2022 23:00:00 GMT+0000
    },
    "SPAN1": {
        "start": 1640995200,  # Sat Jan 01 2022 00:00:00 GMT+0000
        "end": 1656633600     # Fri Jul 01 2022 00:00:00 GMT+0000
    },
    "SPAN2": {
        "start": 1648771200,  # Fri Apr 01 2022 00:00:00 GMT+0000
        "end": 1664582400     # Sat Oct 01 2022 00:00:00 GMT+0000
    }
}
