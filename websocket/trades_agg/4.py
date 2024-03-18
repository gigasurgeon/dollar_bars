#!/usr/bin/env python
import logging
from binance.um_futures import UMFutures
from binance.lib.utils import config_logging

config_logging(logging, logging.DEBUG)

um_futures_client = UMFutures()

change_list = um_futures_client.ticker_24hr_price_change()

for i in range(len(change_list)):
    change_list[i]['priceChange'] = float(change_list[i]['priceChange'])
    change_list[i]['priceChangePercent'] = float(change_list[i]['priceChangePercent'])
    change_list[i]['weightedAvgPrice'] = float(change_list[i]['weightedAvgPrice'])
    change_list[i]['lastPrice'] = float(change_list[i]['lastPrice'])
    change_list[i]['lastQty'] = float(change_list[i]['lastQty'])
    change_list[i]['openPrice'] = float(change_list[i]['openPrice'])
    change_list[i]['highPrice'] = float(change_list[i]['highPrice'])
    change_list[i]['lowPrice'] = float(change_list[i]['lowPrice'])
    change_list[i]['volume'] = float(change_list[i]['volume'])
    change_list[i]['quoteVolume'] = float(change_list[i]['quoteVolume'])
    change_list[i]['dollar_volume_traded_in_millions'] = round(change_list[i]['weightedAvgPrice']*change_list[i]['volume']/1000000)


change_list = sorted(change_list, key=lambda i: i['priceChangePercent'], reverse=True)

for i in change_list:
    print(f"{i['symbol']}  {i['priceChangePercent']}  ${i['dollar_volume_traded_in_millions']}")

# print(change_list)


