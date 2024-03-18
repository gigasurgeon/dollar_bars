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
collection_name = 'maticusdt'

db = client[database_name]
collection = db[collection_name]




def divide_in_batches(trade_ids):
    trade_ids = sorted(trade_ids)

    trade_batches = []
    tmp_trades = []

    for i in range(0, len(trade_ids)-1):
        cur = trade_ids[i]
        next = trade_ids[i+1]

        if cur+1==next:
            tmp_trades.append(cur)
        else:
            tmp_trades.append(cur)
            trade_batches.append(tmp_trades)

    trade_batches.append([trade_ids[-1]])

    starting_trade_ids = []

    for batch in trade_batches:
        if len(batch)==0:
            continue

        if len(batch)<=1000:
            starting_trade_ids.append(batch[0])
            continue

        for idx in range(0, len(batch),1000):
            starting_trade_ids.append(batch[idx])

    return starting_trade_ids


def update_historical_trades(trade_ids):
    # starting_trade_ids = divide_in_batches(trade_ids)



    global invisible_trade_ids

    count = 0

    updated_trade_ids = {trade_id: False for trade_id in trade_ids}


    for trade_id in trade_ids:
        if trade_id in invisible_trade_ids:
            continue

        if updated_trade_ids[trade_id]==True:
            continue


        fetched_trades = um_futures_client.historical_trades("MATICUSDT", **{"fromId": trade_id, 'limit':500})
        fetched_trade_ids = [i['id'] for i in fetched_trades]



        for i in fetched_trades:

            i['time_local'] = datetime.datetime.fromtimestamp(i['time']/1000).strftime('%d-%m-%Y %H:%M:%S.%f')
            i['_id'] = i['id']

            query = {'_id': i['_id']}
            update = {'$set': i}
            a = collection.update_one(query, update, upsert=True)

            updated_trade_ids[i['id']]=True

            if a.upserted_id is not None:
                count+=1

        print(f'successfully_inserted: {count}/{len(trade_ids)}  invisible_trades: {len(invisible_trade_ids)}')

        if trade_id not in fetched_trade_ids:
            invisible_trade_ids.append(trade_id)
            f = open('invisible_trade_ids.txt', 'w')
            for line in invisible_trade_ids:
                f.write(f'{line}\n')
            f.close()


    return invisible_trade_ids, count


invisible_trade_ids = []

if os.path.exists('invisible_trade_ids.txt'):
    invisible_trade_ids = open('invisible_trade_ids.txt','r').readlines()
    invisible_trade_ids = [line.strip() for line in invisible_trade_ids]


while True:
    count = 0
    previous_id = None
    missing=False
    cursor = collection.find({}, {'id': 1}).sort('id', 1)

    missing_ids = []

    for document in cursor:
        current_id = document['id']
        count+=1

        if previous_id is not None and current_id != previous_id + 1:
            for i in range(previous_id+1, current_id):

                if i not in invisible_trade_ids:
                    missing_ids.append(i)

        previous_id = current_id

    print(f'missing_trades: {len(missing_ids)}')

    invisible_trade_ids, success_count = update_historical_trades(missing_ids)
    print(f'invisible_trades: {len(invisible_trade_ids)} , successfully_inserted: {success_count}')


    exit()
client.close()
