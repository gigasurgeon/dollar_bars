import datetime
from pymongo import MongoClient
import subprocess
import json
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient


host_ip = subprocess.check_output('cat /etc/resolv.conf | grep nameserver', shell=True, text=True).split('nameserver')[1].strip()
client = MongoClient(f'mongodb://{host_ip}:27017')
database_name = 'crypto'
db = client[database_name]


SYMBOLS = ['maticusdt', 'bnbusdt', 'btcusdt']
streams = [f'{i}@aggTrade' for i in SYMBOLS]


def insert_tick(_, message):
    if 'data' not in message:
        print(f'ERROR: {message}')
        return

    tick = json.loads(message)
    tick = tick['data']
    tick['time_local'] = datetime.datetime.fromtimestamp(tick['T']/1000).strftime('%d-%m-%Y %H:%M:%S.%f')
    tick['_id'] = tick['a']

    query = {'_id': tick['_id']}
    update = {'$set': tick}

    symbol = tick['s'].lower()
    db[symbol].update_one(query, update, upsert=True)
    print(tick)


def on_open(message):
    print(f'opened stream')

def on_close(_, message):
    print(f'closing stream: {message}')

def on_error(_, message):
    print(f'closing stream')


my_client = UMFuturesWebsocketClient(on_message=insert_tick, on_open=on_open, on_error=on_error, on_close=on_close, is_combined=True)
my_client.subscribe(stream=streams)

