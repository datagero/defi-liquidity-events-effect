import time
import math
import json
from uniswap_queries import lp_queries, uniswap_transaction_extract

# Timestamp for proposed start and end analysis period
start = 1640995200 #Sat Jan 01 2022 00:00:00 GMT+0000
end = 1656633600 #Fri Jul 01 2022 00:00:00 GMT+0000

start = 1687129200 #Sun Jun 18 2023 23:00:00 GMT+0000
end = 1687215600 #Mon Jun 19 2023 23:00:00 GMT+0000

start = 1648771200 #Fri Apr 01 2022 00:00:00 GMT+0000
end = 1664582400 #Sat Oct 01 2022 00:00:00 GMT+0000


tiers = {500: None, 3000: None}
for tier in tiers:
    current_timestamp = int(time.time())
    twenty_four_hours_ago = current_timestamp - 24 * 60 * 60
    tiers[tier] = {}
    for lp_type, lp_query in lp_queries.items():
        tiers[tier][lp_type] = uniswap_transaction_extract(lp_type, lp_query, twenty_four_hours_ago, current_timestamp, tier)
        # tiers[tier][lp_type] = uniswap_transaction_extract(lp_type, lp_query, start, end, tier)

pass

# Save tiers to json file
# json.dump(tiers, open("Data/WBTC-WETH.json", "w"))



# Below some basic analysis to validate against https://www.geckoterminal.com/eth/pools/0xcbcdf9626bc03e24f779434178a73a0b4bad62ed
swaps = tiers[3000]['swaps']
swaps = [{**swap, **{'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(swap['timestamp'])))}} for swap in swaps]
latest_swap = swaps[0]
swap_price = abs(float(latest_swap['amountUSD']) / float(latest_swap['amount0']))
swap_price = abs(float(latest_swap['amount1']) / float(latest_swap['amount0']))
latest_swap['timestamp']


print(len(tiers[500]['mints']))
print(len(tiers[3000]['mints']))

mints = tiers[500]['mints']
mints = [{**mint, **{'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(mint['timestamp'])))}} for mint in mints]
latest_mint = mints[0]
# price = abs(float(latest_mint['amount1']) / float(latest_mint['amount0']))
width = int(latest_mint['tickUpper']) - int(latest_mint['tickLower'])
mint_price = math.sqrt(int(latest_mint['tickUpper']) * int(latest_mint['tickLower']))

for mint in mints:
    width = int(latest_mint['tickUpper']) - int(latest_mint['tickLower'])
    print(math.sqrt(abs(int(mint['tickUpper']) * int(mint['tickLower']))))
    if mint['amount0'] == '0':
        continue
    print(float(mint['amountUSD'])/float(mint['amount1']))



    mint_price = math.sqrt(int(latest_mint['tickUpper']) * int(latest_mint['tickLower']))
    mint_weth_price = mint_price / float(mint['amountUSD']) * 2
    print(mint_weth_price)
latest_mint['timestamp']


burns = tiers[3000]['burns']
burns = [{**burn, **{'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(burn['timestamp'])))}} for burn in burns]
latest_burn = burns[0]
# price = abs(float(latest_burn['amount1']) / float(latest_burn['amount0']))
width = int(latest_burn['tickUpper']) - int(latest_burn['tickLower'])
burn_price = math.sqrt(int(latest_burn['tickUpper']) * int(latest_burn['tickLower']))
latest_burn['timestamp']


for transaction in swaps:
    price = abs(float(transaction['amountUSD']) / float(transaction['amount0']))
    if price < lowest_price:
        lowest_price = price
    if price > highest_price:
        highest_price = price

print("Lowest Price:", lowest_price)
print("Highest Price:", highest_price)

sell_amount0_count = sum(float(tx["amount0"]) > 0 for tx in swaps)
buy_amount0_count = sum(float(tx["amount0"]) < 0 for tx in swaps)
zero_amount0_count = sum(float(tx["amount0"]) == 0 for tx in swaps)
