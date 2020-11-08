import csv
import json
import requests
import calendar
import time
import pandas as pd
import math
from AlmaIndicator import ALMAIndicator
from constants.stocklist import stock_list
from constants.stocklist import remove_list

# Database
import pymongo
from flask import Flask
from flask_cors import CORS

# Start of test - 20160101
ots = '1451581261'
cts = calendar.timegm(time.gmtime())
volumeArray = []
valueArray = []


def compute_indicators(list):
    df = pd.DataFrame(list)
    df.columns = ['code', 'timestamp', 'open',
                  'high', 'low', 'close', 'volume', 'value']
    alma_indicator = ALMAIndicator(close=df['close'])

    df['ma20'] = df.close.rolling(20).mean()
    df['ma50'] = df.close.rolling(50).mean()
    df['ma100'] = df.close.rolling(100).mean()
    df['alma'] = alma_indicator.alma()

    result = df.to_json(orient="records")
    parsed = json.loads(result)
    # temporary solution
    # should have gui to trigger delete
    # should also implement upsert
    saveStocks(parsed)


connection_url = 'mongodb+srv://admin:admin@cluster0.70gug.mongodb.net/exercise-tracker?retryWrites=true&w=majority'
app = Flask(__name__)
client = pymongo.MongoClient(connection_url)

# Database
Database = client.get_database('stock-analyzer')
# Table
stocks_table = Database.stocks


def saveStocks(stocks):
    stocks_table.insert_many(stocks)


def deleteAllStocks():
    stocks_table.delete_many({})


def deleteStocks(query):
    stocks_table.delete_many(query)

# if len(remove_list):
# 	my_query = { "code": { "$in" : {str(remove_list)}}}
# 	deleteStocks(my_query)


for stock in stock_list:
    i = 0
    j = 0
    ohlctValue = []
    ohlctVolume = []
    volumeArray = []
    valueArray = []

    volumeApi = 'https://api.kisschart.com/api/chart/history?symbol={0}&resolution=D&from={1}&to={2}&firstDataRequest=true'
    valueApi = 'https://api.kisschart.com/api/chart/history?symbol={0}%23VALUE&resolution=D&from={1}&to={2}&firstDataRequest=true'

    # OHLC, timestamp and Volume
    print('calling', volumeApi.format(stock, ots, cts))

    try:
        ohlcVolume = requests.get(volumeApi.format(stock, ots, cts))
    except:
        print('Error on getting volume for ', stock)

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
    print('calling', valueApi.format(stock, ots, cts))

    try:
        ohlcValue = requests.get(valueApi.format(stock, ots, cts))
    except:
        print('Error on getting value for ', stock)

    jsonValue = ohlcValue.content.decode('utf8').replace("'", '"')
    valueJson = json.loads(jsonValue)

    # Value
    value = valueJson['v']
    value_time_stamp = valueJson['t']

    # Volume Loop
    for time in volume_time_stamp:
        ohlctVolume = [stock, time, openPrice[i], high[i],
                       low[i], close[i], volume[i]]
        volumeArray.append(ohlctVolume)
        i = i + 1

    # Value Loop
    for time in value_time_stamp:
        ohlctValue = [time, value[j]]
        valueArray.append(ohlctValue)
        j = j + 1

    for vol in volumeArray:
        for val in valueArray:
            if vol[1] == val[0]:
                vol.append(val[1])

    compute_indicators(volumeArray)
