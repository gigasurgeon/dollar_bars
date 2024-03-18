import os
os.environ["LANG"]='en_US.UTF-8'
import pandas as pd
import numpy as np
from pymongo import MongoClient
import subprocess
import math

import pandas as pd
from lightweight_charts import Chart


def generate_dollar_bars(tick_data, target_dollar_value):
    dollar_bars_idx = [0] # adding 0 temporarily
    current_dollar_value = 0

    for index, row in tick_data.iterrows():
        dollar_value = row['price']*row['qty']
        if current_dollar_value + dollar_value >= target_dollar_value:
            dollar_bars_idx.append(index)
            current_dollar_value = 0
        else:
            current_dollar_value += dollar_value

    dollar_bars_idx = list(dict.fromkeys(dollar_bars_idx))
    # dollar_bars_idx = [0] + dollar_bars_idx # adding 0 temporarily


    lows = []
    highs = []
    dollars_traded = []

    for i in range(len(dollar_bars_idx)-1):
        start_index = dollar_bars_idx[i]
        end_index = dollar_bars_idx[i+1]

        low = tick_data.loc[start_index:end_index]['price'].min()
        high = tick_data.loc[start_index:end_index]['price'].max()
        dollar_volume = round(tick_data.loc[start_index:end_index]['dollars_traded'].sum())

        lows.append(low)
        highs.append(high)
        dollars_traded.append(dollar_volume)

    dollar_bars_idx = dollar_bars_idx[1:] # removing the temporary 0

    # Extract dollar bars from the original tick data
    dollar_bars_data = tick_data.loc[dollar_bars_idx].copy()

    # Add open, high, low, and close columns
    dollar_bars_data['open'] = tick_data['price'][dollar_bars_idx].shift(1)
    dollar_bars_data['high'] = highs#dollar_bars_data['open'] #tick_data['price'][dollar_bars].max()
    dollar_bars_data['low'] = lows #dollar_bars_data['high'] #tick_data['price'][dollar_bars].min()
    dollar_bars_data['close'] = tick_data['price'][dollar_bars_idx]
    dollar_bars_data['dollars_traded'] = dollars_traded

    return dollar_bars_data

def calculate_sma(df, ma_type, period):
    if ma_type.lower()=='sma':
        return pd.DataFrame({'time': df['time'], f'sma_{period}': df['close'].rolling(window=period).mean()})

    elif ma_type.lower()=='ema':
        return pd.DataFrame({'time': df['time'], f'ema_{period}': df['close'].ewm(span=period).mean()})

    return None

def add_sma_and_ema(df):
    #for periods 20, 30 and 50
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['sma_30'] = df['close'].rolling(window=30).mean()
    df['sma_50'] = df['close'].rolling(window=50).mean()

    df['ema_20'] = df['close'].ewm(span=20).mean()
    df['ema_30'] = df['close'].ewm(span=30).mean()
    df['ema_50'] = df['close'].ewm(span=50).mean()

    return df

def batch_df(cursor, batch_size):
    batch = []
    for doc in cursor:
        batch.append(doc)
        if batch and not len(batch) % batch_size:
            yield batch
            batch = []

    if batch:
        yield batch

################################################################################

host_ip = subprocess.check_output('cat /etc/resolv.conf | grep nameserver', shell=True, text=True).split('nameserver')[1].strip()

# Connect to MongoDB
client = MongoClient(f'mongodb://{host_ip}:27017')
database_name = 'crypto'
collection_name = 'maticusdt'

db = client[database_name]
collection = db[collection_name]
documents = collection.find({}).sort('_id', 1)

# documents = documents[:5000000]

total_documents = collection.count_documents({})
print(f'total_documents: {total_documents}')


batch_size = 1000000
total_batches = math.ceil(total_documents/batch_size)

tick_data = pd.DataFrame()
for idx, batch in enumerate(batch_df(documents, batch_size)):
    print(f'processing batch: {idx+1}/{total_batches}')
    tick_data = tick_data._append(batch, ignore_index=True)

# a: Aggregate tradeId, p: Price, q: Quantity, f: First tradeId, l: Last tradeId, T: Timestamp, m: Was the buyer the maker
tick_data['price'] = tick_data['p'].astype(float)
tick_data['qty'] = tick_data['q'].astype(float)
tick_data['dollars_traded'] = tick_data['price']*tick_data['qty']
tick_data['timestamp'] = pd.to_datetime(tick_data['T'], unit='ms')
# tick_data = tick_data.drop(['E', 'e'], axis=1)  #dropping unwanted columns from df, E -> event_time and e -> event_type are inserted while using websocket

target_dollar_values = [25000000, 5000000] #[25000000, 5000000]
to_remove_columns = ['E', 'e', 's', 'timestamp']

dollar_bars_dict = {}

for target_dollar_value in target_dollar_values:
    print(f'creating chart for ${target_dollar_value}')

    dollar_bars = generate_dollar_bars(tick_data, target_dollar_value)
    dollar_bars['volume'] = dollar_bars['qty']
    dollar_bars['time'] = pd.to_datetime(dollar_bars['timestamp']).dt.floor('S')

    for column in to_remove_columns:
        if column in dollar_bars:
            dollar_bars = dollar_bars.drop([column], axis=1)

    print(dollar_bars.isna().sum())
    print(f'before dropna: {dollar_bars.shape}')
    dollar_bars = dollar_bars.dropna()
    dollar_bars = dollar_bars.drop_duplicates(subset='time')
    dollar_bars = dollar_bars.reset_index(drop=True)

    #adding sma and ema to dollar_bars df
    dollar_bars = add_sma_and_ema(dollar_bars)

    print(f'after dropna: {dollar_bars.shape}')
    print(f'target_dollar_value: {target_dollar_value}   dollar_bars_generated: {dollar_bars.shape}\n')

    dollar_bars_dict[target_dollar_value] = dollar_bars


###################################### plotting charts


main_chart = Chart(width=1820, height=980, inner_width=1, inner_height=0.5)
# main_chart.time_scale(visible=False)
chart2 = main_chart.create_subchart(width=1, height=0.5, sync=True, sync_crosshairs_only=True)

# charts = {25000000:main_chart, 5000000:chart2}
charts = {25000000:main_chart, 5000000:chart2}

# print(dollar_bars.head())
# exit()
for dollar_value in charts:
    charts[dollar_value].set(dollar_bars_dict[dollar_value])
    charts[dollar_value].precision(5)

    line = charts[dollar_value].create_line('sma_20', color='#ef5350', width=1, price_label=False, price_line=False)
    sma_20_df = calculate_sma(dollar_bars_dict[dollar_value], ma_type='sma', period=20)
    line.set(sma_20_df)

    line = charts[dollar_value].create_line('sma_30', color='#66bb6a', width=1, price_label=False, price_line=False)
    sma_30_df = calculate_sma(dollar_bars_dict[dollar_value], ma_type='sma', period=30)
    line.set(sma_30_df)

    line = charts[dollar_value].create_line('sma_50', color='#4dd0e1', width=1, price_label=False, price_line=False)
    sma_50_df = calculate_sma(dollar_bars_dict[dollar_value], ma_type='sma', period=50)
    line.set(sma_50_df)

    line = charts[dollar_value].create_line('ema_20', color='#ef5350', width=1, price_label=False, price_line=False)
    ema_20_df = calculate_sma(dollar_bars_dict[dollar_value], ma_type='ema', period=20)
    line.set(ema_20_df)

    line = charts[dollar_value].create_line('ema_30', color='#66bb6a', width=1, price_label=False, price_line=False)
    ema_30_df = calculate_sma(dollar_bars_dict[dollar_value], ma_type='ema', period=30)
    line.set(ema_30_df)

    line = charts[dollar_value].create_line('ema_50', color='#4dd0e1', width=1, price_label=False, price_line=False)
    ema_50_df = calculate_sma(dollar_bars_dict[dollar_value], ma_type='ema', period=50)
    line.set(ema_50_df)


main_chart.show(block=True)



# chart = Chart(debug=True)
# chart.set(dollar_bars)
# chart.precision(5)

# line = chart.create_line('sma_20', color='#ef5350', width=1, price_label=False, price_line=False)
# sma_20_df = calculate_sma(dollar_bars, ma_type='sma', period=20)
# line.set(sma_20_df)

# line = chart.create_line('sma_30', color='#66bb6a', width=1, price_label=False, price_line=False)
# sma_30_df = calculate_sma(dollar_bars, ma_type='sma', period=30)
# line.set(sma_30_df)

# line = chart.create_line('sma_50', color='#4dd0e1', width=1, price_label=False, price_line=False)
# sma_50_df = calculate_sma(dollar_bars, ma_type='sma', period=50)
# line.set(sma_50_df)

# line = chart.create_line('ema_20', color='#ef5350', width=1, price_label=False, price_line=False)
# ema_20_df = calculate_sma(dollar_bars, ma_type='ema', period=20)
# line.set(ema_20_df)

# line = chart.create_line('ema_30', color='#66bb6a', width=1, price_label=False, price_line=False)
# ema_30_df = calculate_sma(dollar_bars, ma_type='ema', period=30)
# line.set(ema_30_df)

# line = chart.create_line('ema_50', color='#4dd0e1', width=1, price_label=False, price_line=False)
# ema_50_df = calculate_sma(dollar_bars, ma_type='ema', period=50)
# line.set(ema_50_df)


# chart.show(block=True)
######################
