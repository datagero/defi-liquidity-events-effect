import time
import json
from uniswap_queries import lp_queries, uniswap_transaction_extract

# Timestamp for proposed start and end analysis period
start = 1640995200 #Sat Jan 01 2022 00:00:00 GMT+0000
end = 1656633600 #Fri Jul 01 2022 00:00:00 GMT+0000

tiers = {500: None, 3000: None}
for tier in tiers:
    current_timestamp = int(time.time())
    twenty_four_hours_ago = current_timestamp - 24 * 60 * 60
    tiers[tier] = {}
    for lp_type, lp_query in lp_queries.items():
        tiers[tier][lp_type] = uniswap_transaction_extract(lp_type, lp_query, twenty_four_hours_ago, current_timestamp, tier)

pass

# Save tiers to json file
json.dump(tiers, open("Data/WBTC-WETH.json", "w"))



# Below some basic analysis to validate against https://www.geckoterminal.com/eth/pools/0xcbcdf9626bc03e24f779434178a73a0b4bad62ed
transactions = tiers[3000]['swaps']

lowest_price = abs(float(transactions[0]['amountUSD']) / float(transactions[0]['amount0']))
highest_price = abs(float(transactions[0]['amountUSD']) / float(transactions[0]['amount0']))

for transaction in transactions:
    price = abs(float(transaction['amountUSD']) / float(transaction['amount0']))
    if price < lowest_price:
        lowest_price = price
    if price > highest_price:
        highest_price = price

print("Lowest Price:", lowest_price)
print("Highest Price:", highest_price)

sell_amount0_count = sum(float(tx["amount0"]) > 0 for tx in transactions)
buy_amount0_count = sum(float(tx["amount0"]) < 0 for tx in transactions)
zero_amount0_count = sum(float(tx["amount0"]) == 0 for tx in transactions)
