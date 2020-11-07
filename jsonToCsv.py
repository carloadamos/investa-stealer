import csv
import json
import requests
import calendar
import time
from constants.stocklist import test_list

# Start of test - 20160101
ots = '1451581261'
cts = calendar.timegm(time.gmtime())
volumeArray = []
valueArray = []

for stock in test_list:
    i = 0
    j = 0
    volumeApi = 'https://api.kisschart.com/api/chart/history?symbol={0}&resolution=D&from={1}&to={2}&firstDataRequest=true'
    valueApi = 'https://api.kisschart.com/api/chart/history?symbol={0}%23VALUE&resolution=D&from={1}&to={2}&firstDataRequest=true'

    # OHLC, timestamp and Volume
    ohlcVolume = requests.get(volumeApi.format(stock, ots, cts))
    jsonVolume = ohlcVolume.content.decode('utf8').replace("'", '"')
    volumeJson = json.loads(jsonVolume)

    # Volume
    openPrice = volumeJson['o']
    high = volumeJson['h']
    low = volumeJson['l']
    close = volumeJson['c']
    volume = volumeJson['v']
    volume_time_stamp = volumeJson['t']

    # OHLC, timestamp and Value
    ohlcValue = requests.get(valueApi.format(stock, ots, cts))
    jsonValue = ohlcValue.content.decode('utf8').replace("'", '"')
    valueJson = json.loads(jsonValue)

    # Value
    value = valueJson['v']
    value_time_stamp = valueJson['t']

    # Volume Loop
    for time in volume_time_stamp:
        ohlctVolume = [time, openPrice[i], high[i],
                       low[i], close[i], volume[i]]
        volumeArray.append(ohlctVolume)

        # for x in volumeArray:
        #     print(x)

        i = i + 1

        # Value Loop
    for time in value_time_stamp:
        ohlctValue = [time, value[j]]
        valueArray.append(ohlctValue)

        # for x in valueArray:
        # 		print(x)

        j = j + 1

    for vol in volumeArray:
        for val in valueArray:
            if vol[0] == val[0]:
                vol.append(val[1])

    for x in volumeArray:
        print(x)

# print(stockArray)

# data_file = open(f"{stock}.csv", "w")
# writer = csv.writer(data_file)

# writer.writerow(openPrice)
