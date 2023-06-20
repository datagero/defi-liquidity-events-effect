# NOTE -> Currently not working

import requests
from datetime import datetime, timedelta

# Define the API endpoint for fetching recent trades
url = "https://api.binance.com/api/v3/trades"

# Set the required parameters
symbol = "ETHBTC"  # ETH-BTC trading pair

# Calculate the timestamps for the start and end of the desired 24-hour period
end_time = int(datetime.now().timestamp() * 1000)
start_time = int((datetime.now() - timedelta(hours=24)).timestamp() * 1000)

# Construct the query parameters
params = {
    "symbol": symbol,
    "startTime": start_time,
    "endTime": end_time,
    "limit": 1000  # Maximum number of data points to retrieve per request (up to 1000)
}

transactions = []

# Send the initial API request
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()

    # Process the retrieved data
    transactions.extend(data)

    # Check if there are more trades to fetch
    while len(data) == 1000:
        # Update the start time for the next request
        start_time = data[-1]['time'] + 1

        # Update the query parameters
        params['startTime'] = start_time

        # Send the next API request
        response = requests.get(url, params=params)

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()

            # Append the new trades to the existing list
            transactions.extend(data)
        else:
            print("Request failed with status code:", response.status_code)
            break

else:
    print("Request failed with status code:", response.status_code)

pass