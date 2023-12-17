# :file_folder: Project Overview: Data

The project involves data extraction from Decentralized Exchanges (DEX) and Centralized Exchanges (CEX) for a duration of 6 months. The data is then matched and processed for further analysis.

# :inbox_tray: Data Downloads 

The download scripts are stored in separate folders based on the source:

- `binance/`
- `etherscan/`
- `uniswap/`

For each, you'll need to run a download script, followed by a cleaning script. Note, for binance the download is made through CLI and we require to unzip/consolidate it before running cleaning scripts, see section below for further info.

## :globe_with_meridians: DEX - Uniswap's The Graph API 

API Endpoint: [Uniswap The Graph API](https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3)

This API provides transaction details, trading volumes, and block information. We focus on the Uniswap v3 WBTC-WETH liquidity pools of 500 and 3000. More attributes are available, refer to the [full list here](https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3/graphql?query=query+MyQuery+%7B%0A++pools%0A%7D).

Example `transaction_data`:
```json
{
    "id": "",
    "timestamp": "",
    "amount0": "",
    "amount1": "",
    "amountUSD": ""
}
```

## :globe_with_meridians: Etherscan API 

API Endpoint: [Etherscan API](https://api.etherscan.io/api)

This API is used to extract corresponding transaction data from Etherscan based on the transaction hashes obtained from Uniswap.

Example `transaction_data`:
```json
{
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
```

## :globe_with_meridians: CEX - Binance Data Download 

Scripts were taken from the Binance [GitHub repository](https://github.com/binance/binance-public-data/blob/master/python/README.md) to download trades.

Command used for download the transactions on a .zip file:
```bash
python Code/DataSourcingCleaning/binance/download-trade.py -t "spot" -s "ETHBTC" -skip-monthly 1
```

Then, by running `scope-downloaded-daily-data.py`, the files get unzipped on the `Data\` folder and consolidated into a single file: `binance.csv`
Finally, need to run the `binance-cleaning.py` script.

The schema for binance data is specified in [Binance Docs](https://binance-docs.github.io/apidocs/spot/en/#old-trade-lookup-market_data).

Example `trade_data`:
```json
[
  {
    "id": 28457,
    "price": "4.00000100",
    "qty": "12.00000000",
    "quoteQty": "48.000012",
    "time": 1499865549590,  // Trade executed timestamp, as same as `T` in the stream
    "isBuyerMaker": true,
    "isBestMatch": true
  }
]
```
Note: The timestamp will be approximated to corresponding DEX blocks.
