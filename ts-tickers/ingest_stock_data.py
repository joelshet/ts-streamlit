import csv
import pandas as pd
import config
from time import sleep

from pgcopy import CopyManager
import psycopg2

conn = psycopg2.connect(database=config.DB_NAME,
    host=config.DB_HOST,
    user=config.DB_USER,
    password=config.DB_PASS,
    port=config.DB_PORT)

COLUMNS = ('time', 'symbol', 'price_open', 'price_close', 'price_low', 'price_high', 'trading_volume')

with open('symbols.csv') as f:
    reader = csv.reader(f)
    symbols = [row[0] for row in reader]

def fetch_stock_data(symbol, month):
    interval = '1min'
    slice = "year1month" + str(month) if month <= 12 else "year2month1" + str(month)

    apikey = config.APIKEY

    CSV_URL = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={symbol}&interval={interval}&slice={slice}&apikey={apikey}'

    # print(CSV_URL)
    df = pd.read_csv(CSV_URL)

    df['symbol'] = symbol
    df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S')

    df = df.rename(columns={'time':'time',
                            'open': 'price_open',
                            'high': 'price_high',
                            'low': 'price_low',
                            'close': 'price_close',
                            'volume': 'trading_volume'})
    df = df[['time', 'symbol', 'price_open', 'price_close', 'price_low', 'price_high', 'trading_volume']]
    
    return [row for row in df.itertuples(index=False, name=None)]

def test_stock_download():
    test_stock_data = fetch_stock_data("MSFT", 1)
    # print(test_stock_data)

api_counter = 1
time_range = range(1,12)

for symbol in symbols:
    for month in time_range:
        if api_counter % 5 == 0:
            print(f'{api_counter=}, going to sleep now...')
            sleep(61)
        else:
            print(f"{symbol=}, {month=}")
        api_counter += 1
        stock_data = fetch_stock_data(symbol, month)

        mgr = CopyManager(conn, 'stocks_intraday', COLUMNS)

        mgr.copy(stock_data)
        conn.commit()
