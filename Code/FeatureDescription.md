## Features and Definitions

Our project draws inspiration and methodology from the paper *DeFi: Modeling and Forecasting Trading Volume on Uniswap v3 Liquidity Pools* authored by Deborah Miori and Mihai Cucuringu. We have curated a selection of features to analyze within our score, which can be referenced in our source code and outputs.

> Source: Miori, Deborah & Cucuringu, Mihai (2023). DeFi: modeling and forecasting trading volume on Uniswap v3 liquidity pools. [SSRN Abstract](https://ssrn.com/abstract=4445351)

The table below outlines the baseline features, their definitions and whether if used within the scope of this project:

| No. | Variable | Instances | Pool Type | In Scope | Description |
|-----|----------|-----------|-----------|----------|-------------|
| 1.  | bl       | _1, _2, _3 | Same/Other | Yes | Distance in blocks of the previous l ∈ {1, 2, 3} mint operations on the same/other pool as the one where the mint of reference happened. |
| 2.  | sl       | _1, _2, _3 | Same/Other | Yes | Size in USD of the previous l ∈ {1, 2, 3} mint operations on the same/other pool as the one where the mint of reference happened. |
| 3.  | wl       | _1, _2, _3 | Same/Other | Yes | Width in number of ticks of the previous l ∈ {1, 2, 3} mint operations on the same/other pool as the one where the mint of reference happened. |
| 4.  | s0, w0   |  - | Same | Yes | Size in USD, and width in number of ticks, of the current mint operation that we take as reference. |
| 5.  | vol0     | _1, _2, _3 | Same | Yes | Volatility as the standard deviation of the pool price on the same pool as the mint of reference, during the expanding intervals of time 0el with l ∈ {1, 2, 3} that refer to the previous mint operations on the same pool. |
| 6.  | rate-USD-i, rate-count-i | _01, _12, _23 | Same/Other | Yes | Rate of traded volume/count of trades in USD on our WBTC-WETH pools of principal interest, for the intervals i ∈ {01, 12, 23} block ranges with respect to either the latest mint operations and swaps executed on the same/other pool. |
| 7.  | avg-USD-i | _01, _12, _23 | Same/Other | Yes | Average traded volume in USD on our WBTC-WETH pools of principal interest, for the intervals i ∈ {01, 12, 23} block ranges with respect to either the latest mint operations and swaps executed on the same/other pool. |
| 8.  | TVL3000-500, TVL3000/500 | - | Same | No | Latest value of the TVL in the pool 3000 minus/ratio of the TVL in the pool 500. |
| 9.  | eth500-USD-03, eth3000-USD-03 | - | Same | No | Rate of traded volume in USD on the one-hop USDC-WETH pools, over the 03 time interval with e respect to the previous mint operations on the same pool as our mint of reference. |
| 10. | btc500-USD-03, btc3000-USD-03 | - | Same | No | Rate of traded volume in USD on the one-hop WBTC-USDC pools, over the 03 time interval with e respect to the previous mint operations on the same pool as our mint of reference. |
| 11. | eth500-press-03, eth3000-press-03 | - | Same | No | Rate of surplus WETH buying volume in USD on the one-hop USDC-WETH pools, over the 03 time interval e with respect to the previous mint operations on the same pool as our mint of reference. |
| 12. | btc500-press-03, btc3000-press-03 | - | Same | No | Rate of surplus WBTC buying volume in USD on the one-hop WBTC-USDC pools, over the 03 time interval e with respect to the previous mint operations on the same pool as our mint of reference. |
| 13. | binance-btc-i, binance-count-i | _01, _12, _23 | Same | Yes | Relates to Binance data, either the traded volume in BTC or the count of trades, over intervals i ∈ {01, 12, 23}. |
| 14. | ∆(Zsame, Zother), ∆(Zsame, ZBinance) | - | Same | No | Measures the price impact. |

The total number of variables amounts to 59, but only 46 of these are in scope for the provided schema.

In addition, we include the feature binance-midprice. This pertains to the mid-price of trades on Binance over intervals i ∈ {01, 12, 23}.