# Make sure to set cwd to project path to run this script
import sys
import os
sys.path.append(os.getcwd())

from datetime import *
from environment.time_spans import load_run_config, time_spans, generate_date_scope

# Select the desired time span for analysis
# Replace 'DEMO' with 'SPAN1' or 'SPAN2' in run_config as needed
selected_span = load_run_config('environment/run-config.env')
START = time_spans[selected_span]["start"]
END = time_spans[selected_span]["end"]

# Generate scope date
scope_date = generate_date_scope(START, END)

YEARS = ['2017', '2018', '2019', '2020', '2021', '2022', '2023']
INTERVALS = ["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1mo"]
DAILY_INTERVALS = ["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
TRADING_TYPE = ["spot", "um", "cm"]
MONTHS = list(range(1,13))
PERIOD_START_DATE = '2020-01-01'
BASE_URL = 'https://data.binance.vision/'
START_DATE = datetime.strptime(scope_date[0], '%Y-%m-%d').date() #date(int(YEARS[0]), MONTHS[0], 1)
END_DATE = datetime.strptime(scope_date[-1], '%Y-%m-%d').date() #datetime.date(datetime.now())