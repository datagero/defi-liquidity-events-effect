
Data Downloads -> For now, we are doing a 24hr extract and matching. This will be increase to at least 6-month period.

The folders binance/ etherscan/ and uniswap/ contain code used to download the relevant data.

DEX - Uniswap's The Graph API (https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3):

Provides transaction details, trading volumes, and block information.
Specifically, we focus on the Uniswap v3 WBTC-WETH liquidity pools of 500 and 3000. The associated token addresses are WBTC (0x2260fac5e5542a773aa44fbcfedf7c193bc2c599) and WETH (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2).
Additional attributes available from the API include transaction IDs, timestamps, amounts, and USD equivalents.

The count of daily transactions for pool 3000 were compared with volumes as displayed in https://www.geckoterminal.com/eth/pools/0xcbcdf9626bc03e24f779434178a73a0b4bad62ed

More attributes are available - for a full list, refer to: https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3/graphql?query=query+MyQuery+%7B%0A++pools%0A%7D
transaction_data = {
            id
            timestamp
            amount0
            amount1
            amountUSD
        }

Etherscan API (https://api.etherscan.io/api):

This API is used to extract corresponding transaction data from Etherscan based on the transaction hashes obtained from Uniswap.
The data retrieved includes block hashes, block numbers, sender addresses, gas details, transaction hashes, and other related information.

transaction_data = {
    "blockHash": "The hash of the block where this transaction was in. Null when it's a pending log",
    "blockNumber": "The block number where this transaction was in. Null when it's a pending log",
    "from": "The address of the sender",
    "gas": "The gas provided by the sender, encoded as hexadecimal",
    "gasPrice": "The gas price provided by the sender in wei encoded as hexadecimal",
    "maxFeePerGas": "The maximum fee per gas set in the transaction",
    "maxPriorityFeePerGas": "The maximum priority gas fee set in the transaction",
    "hash": "The hash of the transaction",
    "input": "The data sent along with the transaction",
    "nonce": "The number of transactions made by the sender prior to this one encoded as hexadecimal",
    "to": "The address of the receiver. Null when it's a contract creation transaction",
    "transactionIndex": "The integer of the transaction's index position that the log was created from. Null when it's a pending log",
    "value": "The value transferred in wei encoded as hexadecimal",
    "type": "The transaction type",
    "accessList": "A list of addresses and storage keys that the transaction plans to access",
    "chainId": "The chain id of the transaction, if any",
    "v": "The standardized V field of the signature",
    "r": "The R field of the signature",
    "s": "The S field of the signature"
}


CEX - Binance data download.
- Scripts were taken from the binance github repository to download-trades: https://github.com/binance/binance-public-data/blob/master/python/README.md
- The following command was used to trigger downloads of daily zip trades: python Code/binance/download-trade.py -t "spot" -s "ETHBTC" -skip-monthly 1
- Data was extracted and collated into a single file in: XXX

The schema for binance data is specified in: https://binance-docs.github.io/apidocs/spot/en/#old-trade-lookup-market_data
[
  {
    "id": 28457,
    "price": "4.00000100",
    "qty": "12.00000000",
    "quoteQty": "48.000012",
    "time": 1499865549590, // Trade executed timestamp, as same as `T` in the stream
    "isBuyerMaker": true,
    "isBestMatch": true
  }
]

The time will be approximated to correspoinding DEX blocks.