import sys
import csv
import json
import requests
import calendar
import time
import pandas as pd
import math
from AlmaIndicator import ALMAIndicator
from constants.stocklist import stock_list
from stockstats import StockDataFrame

# Database
import pymongo

# Logging
import logging

# Start of test - 20150101
ots = '1420088052'
cts = calendar.timegm(time.gmtime())
volumeArray = []
valueArray = []

# Database
connection_url = 'mongodb+srv://admin:admin@cluster0.70gug.mongodb.net/exercise-tracker?retryWrites=true&w=majority'
client = pymongo.MongoClient(connection_url)
Database = client.get_database('stock-analyzer')
stocks_table = Database.stocks

# Logger
logging.basicConfig(filename='executions.log', level=logging.DEBUG,
                    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


def compute_indicators(list):
    """
    Compute indicators from Panda and StockDataFrame.
    """
    df = pd.DataFrame(list)
    df.columns = ['code', 'timestamp', 'open',
                  'high', 'low', 'close', 'volume', 'value']
    alma_indicator = ALMAIndicator(close=df['close'])

    df['ma20'] = df.close.rolling(20).mean()
    df['ma50'] = df.close.rolling(50).mean()
    df['ma100'] = df.close.rolling(100).mean()
    df['alma'] = alma_indicator.alma()

    sdf = StockDataFrame.retype(df)
    sdf.get('macd')
    # In case I decided to use sdf to formulate sma
    # sdf.get('close_20_sma')
    result = df.to_json(orient="records")
    parsed = json.loads(result)

    # temporary solution
    # should implement upsert
    save_stocks(parsed)


def save_stocks(stocks):
    stocks_table.insert_many(stocks)


def delete_all_stocks():
    stocks_table.delete_many({})


def delete_stock(query):
    stocks_table.delete_many(query)


def process_ohlc_volume(code, url):
    print('retrieving data of ', code)
    ohlcVolume = requests.get(url.format(code, ots, cts))
    jsonVolume = ohlcVolume.content.decode('utf8').replace("'", '"')

    return json.loads(jsonVolume)


def process_ohlc_value(code, url):
    print('retrieving data of ', code)
    ohlcValue = requests.get(url.format(code, ots, cts))
    jsonValue = ohlcValue.content.decode('utf8').replace("'", '"')

    return json.loads(jsonValue)


def process_retrieval():
    for stock in stock_list:
        i = 0
        j = 0
        ohlctValue = []
        ohlctVolume = []
        volumeArray = []
        valueArray = []

        volumeUrl = 'https://api.kisschart.com/api/chart/history?symbol={0}&resolution=D&from={1}&to={2}&firstDataRequest=true'
        valueUrl = 'https://api.kisschart.com/api/chart/history?symbol={0}%23VALUE&resolution=D&from={1}&to={2}&firstDataRequest=true'

        try:
            # Retrieve from Atlas
            volumeJson = process_ohlc_volume(stock, volumeUrl)
            valueJson = process_ohlc_value(stock, valueUrl)

            # Process fields
            openPrice = volumeJson['o']
            high = volumeJson['h']
            low = volumeJson['l']
            close = volumeJson['c']
            volume = volumeJson['v']
            volume_time_stamp = volumeJson['t']
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

            # Combine timestamp, ohlc, volume and value
            for vol in volumeArray:
                for val in valueArray:
                    if vol[1] == val[0]:
                        vol.append(val[1])

            compute_indicators(volumeArray)
        except:
            logging.error('Error on {0}'.format(stock))
            print('Error on {0}'.format(stock))

    logging.info('Finished stocks data retrieval')
    print('Finished stocks data retrieval')


# Program start
if len(sys.argv) > 1:
    if sys.argv[1] == 'delall':
        del_confirmation = input(
            "Are you sure you want to delete all stocks in server? [Y/N] ")
        if del_confirmation == 'Y' or del_confirmation == 'y':
            logging.info('Deleting all stocks')
            print('Deleting all stocks')
            delete_all_stocks()
            if len(sys.argv) > 2:
                if sys.argv[2] == 'get':
                    logging.info('Starting data retrieval process.')
                    print('Starting data retrieval process.')
                    process_retrieval()
        else:
            logging.info('Stopping program.')
            print('Stopping program.')
    else:
        print('Please make sure you removed all `ok` stocks from list')
        print('before triggering stocks data retrieval')
        proceed = input('Proceed? [Y/N] ')
        if proceed == 'Y' or proceed == 'y':
            process_retrieval()
        else:
            logging.info('Stopping program. Please fix arguments.')
            print('Stopping program. Please fix arguments.')

else:
    print('Use `delall` to remove all stocks')
    print('Use `get` to get all stocks from list')
    print('`delall` should come first before `get`')
