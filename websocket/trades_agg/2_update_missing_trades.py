from pymongo import MongoClient
import subprocess
from binance.um_futures import UMFutures
import datetime
import os

binance_api_key = 'aol3BAYNi4Z25Unio3rYEq9USJYbmO1D4gFXX4AeWqeodIbAZu8UY0QxDMyanV9m'
um_futures_client = UMFutures(key=binance_api_key)
host_ip = subprocess.check_output('cat /etc/resolv.conf | grep nameserver', shell=True, text=True).split('nameserver')[1].strip()
client = MongoClient(f'mongodb://{host_ip}:27017')
database_name = 'crypto'
db = client[database_name]

SYMBOLS = ['maticusdt', 'bnbusdt', 'btcusdt']


def update_historical_trades(symbol, trade_ids):
    global invisible_trade_ids

    count = 0
    updated_trade_ids = {trade_id: False for trade_id in trade_ids}

    for trade_id in trade_ids:
        if trade_id in invisible_trade_ids:
            continue

        if updated_trade_ids[trade_id]==True:
            continue


        fetched_trades = um_futures_client.agg_trades(symbol, **{"fromId": trade_id, 'limit':1000})
        fetched_trade_ids = [i['a'] for i in fetched_trades]


        for i in fetched_trades:
            i['time_local'] = datetime.datetime.fromtimestamp(i['T']/1000).strftime('%d-%m-%Y %H:%M:%S.%f')
            i['_id'] = i['a']

            query = {'_id': i['_id']}
            update = {'$set': i}
            a = db[symbol].update_one(query, update, upsert=True)

            updated_trade_ids[i['a']]=True

            if a.upserted_id is not None:
                count+=1

        print(f'{symbol}  successfully_inserted: {count}/{len(trade_ids)}  invisible_trades: {len(invisible_trade_ids)}')

        if trade_id not in fetched_trade_ids:
            invisible_trade_ids.append(trade_id)
            f = open(f'invisible_trade_ids_{symbol}.txt', 'w')
            for line in invisible_trade_ids:
                f.write(f'{line}\n')
            f.close()


    return invisible_trade_ids, count


for symbol in SYMBOLS:
    invisible_trade_ids = []

    if os.path.exists(f'invisible_trade_ids_{symbol}.txt'):
        invisible_trade_ids = open(f'invisible_trade_ids_{symbol}.txt','r').readlines()
        invisible_trade_ids = [line.strip() for line in invisible_trade_ids]

    count = 0
    previous_id = None
    missing=False
    cursor = db[symbol].find({}, {'_id': 1}).sort('_id', 1)

    missing_ids = []

    for document in cursor:
        current_id = document['_id']
        count+=1

        if previous_id is not None and current_id != previous_id + 1:
            for i in range(previous_id+1, current_id):
                if i not in invisible_trade_ids:
                    missing_ids.append(i)

        previous_id = current_id

    print(f'{symbol} missing_trades: {len(missing_ids)}')
    invisible_trade_ids, success_count = update_historical_trades(symbol, missing_ids)
    print(f'{symbol}  invisible_trades: {len(invisible_trade_ids)}, successfully_inserted: {success_count}')

client.close()
