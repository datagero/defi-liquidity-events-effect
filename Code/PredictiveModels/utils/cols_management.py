

# Explicitely define all independent variables available
explainable_variables = [
                'blsame_1', 'blsame_2', 'blsame_3',
                'blother_1', 'blother_2', 'blother_3',
                'slsame_1', 'slsame_2', 'slsame_3',
                'slother_1', 'slother_2', 'slother_3',
                'wlsame_1', 'wlsame_2', 'wlsame_3',
                'wlother_1', 'wlother_2', 'wlother_3',
                's0', 'w0',
                'vol_0_1', 'vol_0_2', 'vol_0_3',
                'rate-USD-isame_01', 'rate-USD-isame_12', 'rate-USD-isame_23',
                'rate-USD-iother_01', 'rate-USD-iother_12', 'rate-USD-iother_23',
                'rate-count-isame_01', 'rate-count-isame_12', 'rate-count-isame_23',
                'rate-count-iother_01', 'rate-count-iother_12', 'rate-count-iother_23',
                'avg-USD-isame_01', 'avg-USD-isame_12', 'avg-USD-isame_23',
                'avg-USD-iother_01', 'avg-USD-iother_12', 'avg-USD-iother_23',
                'binance-btc-01', 'binance-btc-12', 'binance-btc-23',
                'binance-count-01', 'binance-count-12', 'binance-count-23',
                ]


# This columns where defined after initial analysis

# High correlation columns
cols_drop_correlated = ['blsame_2', 'blsame_3', 'blother_2', 'blother_3', 'vol_0_1', 'vol_0_2',
                        'binance-count-01', 'binance-count-12', 'binance-count-23',
                        'rate-USD-isame_01', 'rate-USD-isame_12', 'rate-USD-isame_23',
                        'rate-USD-iother_01', 'rate-USD-iother_12', 'rate-USD-iother_23']

# High correlation intervals
cols_aggregate_intervals_range = ['rate-count-isame_', 'rate-count-iother_', 'binance-btc-']