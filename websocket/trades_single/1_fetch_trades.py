import logging
from binance.um_futures import UMFutures
# from binance.lib.utils import config_logging
import datetime
import time
import os
from pymongo import MongoClient
import subprocess


host_ip = subprocess.check_output('cat /etc/resolv.conf | grep nameserver', shell=True, text=True).split('nameserver')[1].strip()

# 022 69181920
# config_logging(logging, logging.DEBUG)
um_futures_client = UMFutures()


# Connect to MongoDB
client = MongoClient(f'mongodb://{host_ip}:27017')
database_name = 'crypto'
collection_name = 'maticusdt'

db = client[database_name]
collection = db[collection_name]


while True:
    ticks = um_futures_client.trades("MATICUSDT", limit=1000)

    for i in ticks:

        i['time_local'] = datetime.datetime.fromtimestamp(i['time']/1000).strftime('%d-%m-%Y %H:%M:%S.%f')
        i['_id'] = i['id']

        query = {'_id': i['_id']}
        update = {'$set': i}
        collection.update_one(query, update, upsert=True)
