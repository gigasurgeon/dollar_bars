import ccxt
import pandas as pd
import plotly.graph_objects as go
import plotly.subplots as sp
import pandas_ta
import os
import numpy as np


def fetch_binance_tick_data_from_csv():
    csv_paths  = [ 'tick_data/MATICUSDT-trades-2023-12-24/MATICUSDT-trades-2023-12-24.csv',
                   'tick_data/MATICUSDT-trades-2023-12-25/MATICUSDT-trades-2023-12-25.csv',
                   'tick_data/MATICUSDT-trades-2023-12-26/MATICUSDT-trades-2023-12-26.csv',
                   'tick_data/MATICUSDT-trades-2023-12-27/MATICUSDT-trades-2023-12-27.csv',
                   'tick_data/MATICUSDT-trades-2023-12-28/MATICUSDT-trades-2023-12-28.csv',
                   'tick_data/MATICUSDT-trades-2023-12-29/MATICUSDT-trades-2023-12-29.csv'
                 ]

    dfs_list = []

    for path in csv_paths:
        tmp_df = pd.read_csv(path)
        dfs_list.append(tmp_df)

    df = pd.concat(dfs_list, axis=0)

    df['timestamp'] = pd.to_datetime(df['time'], unit='ms')
    df['amount'] = df['qty']

    return df

def fetch_binance_tick_data(symbol, limit=1000):
    # Initialize the Binance exchange object
    binance = ccxt.binance()

    # Define the parameters for fetching data
    params = {
        'symbol': symbol,
        'limit': limit,
    }

    # Fetch recent trades data
    trades = binance.fetch_trades(**params)

    # Convert the data to a Pandas DataFrame
    df = pd.DataFrame(trades, columns=['timestamp', 'price', 'amount', 'side'])

    # Convert the timestamp to a readable date format
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

    return df

def generate_dollar_bars(tick_data, target_dollar_value):
    dollar_bars = []
    current_dollar_value = 0

    for index, row in tick_data.iterrows():
        dollar_value = row['price'] * row['amount']

        if current_dollar_value + dollar_value >= target_dollar_value:
            dollar_bars.append(index)
            current_dollar_value = 0
        else:
            current_dollar_value += dollar_value

    # Remove duplicate indices
    dollar_bars = list(dict.fromkeys(dollar_bars))

    # Extract dollar bars from the original tick data
    dollar_bars_data = tick_data.loc[dollar_bars].copy()

    # Add open, high, low, and close columns
    dollar_bars_data['open'] = tick_data['price'][dollar_bars].shift(1)
    dollar_bars_data['high'] = tick_data['price'][dollar_bars].max()
    dollar_bars_data['low'] = tick_data['price'][dollar_bars].min()
    dollar_bars_data['close'] = tick_data['price'][dollar_bars]

    return dollar_bars_data

def add_sma_ema_to_dataframe(df, periods=[20, 30, 50]):
    for period in periods:
        # Calculate SMA
        df[f'sma_{period}'] = df['close'].rolling(window=period).mean()
        # Calculate EMA
        df[f'ema_{period}'] = df['close'].ewm(span=period, adjust=False).mean()
    return df


# def calculate_dollar_bar_momentum_hourly(data):
#     """
#     Calculate momentum in terms of candles printed based on dollar bars within 1-hour windows.

#     Parameters:
#     - data: DataFrame with dollar bars, expected to have 'timestamp' and 'close' columns.

#     Returns:
#     - DataFrame with added 'momentum' column.
#     """
#     # Ensure 'timestamp' is in datetime format and set it as the index
#     tmp_data = data.copy()
#     # tmp_data['timestamp'] = pd.to_datetime(tmp_data['timestamp'])
#     tmp_data = tmp_data.set_index('timestamp')

#     # Resample data into 1-hour intervals and calculate the number of dollar bars within each hour
#     data_resampled = tmp_data.rolling('1H').count()


#     # data_resampled['num_bars'] = data_resampled['close']

#     # # Calculate the rate of change (momentum) in terms of candles printed
#     # data_resampled['momentum'] = data_resampled['num_bars'].pct_change() * 100
#     print(data_resampled)
#     exit()
#     return data_resampled


def calculate_time_based_momentum(data):
    """
    Calculate momentum based on the time difference between consecutive dollar bars.

    Parameters:
    - data: DataFrame with dollar bars, expected to have 'timestamp' and 'close' columns.

    Returns:
    - DataFrame with added 'time_momentum' column.
    """
    # Ensure 'timestamp' is in datetime format and set it as the index
    # data['time'] = data.set_index('timestamp')
    data['time_diff'] = data['timestamp'].diff().dt.total_seconds()/60


    # Calculate the rate of change (momentum) in terms of time difference
    data['inverse_time_momentum'] = 1 / data['time_diff']

    # return data[['close', 'time_diff', 'time_momentum']]
    return data


# def calculate_angles(data, column):
#     price = data['price']#.dropna()
#     ema = data[column]#.dropna()
#     gradient = np.gradient(price)
#     angle_rad = np.arctan2(np.diff(ema), np.diff(gradient))
#     angle_deg = np.degrees(angle_rad)
#     return pd.Series(angle_deg, index=ema.index[1:])


def calculate_angles(data, column):
    price = data[column].dropna()
    gradient = np.gradient(price)
    angle_rad = np.arctan2(np.diff(price), np.diff(gradient))
    angle_deg = np.degrees(angle_rad)
    return pd.Series(angle_deg, index=price.index[1:])


def plot_dollar_bars_candlestick(dollar_bars_data, save_path=None, width=1200, height=800):
    # fig = go.Figure()
    fig = sp.make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3])

    # Calculate angles for Moving Averages
    dollar_bars_data['angle_sma_20'] = calculate_angles(dollar_bars_data, 'sma_20')
    dollar_bars_data['angle_sma_30'] = calculate_angles(dollar_bars_data, 'sma_30')
    dollar_bars_data['angle_sma_50'] = calculate_angles(dollar_bars_data, 'sma_50')
    dollar_bars_data['angle_ema_20'] = calculate_angles(dollar_bars_data, 'ema_20')
    dollar_bars_data['angle_ema_30'] = calculate_angles(dollar_bars_data, 'ema_30')
    dollar_bars_data['angle_ema_50'] = calculate_angles(dollar_bars_data, 'ema_50')

    # print(dollar_bars)


    # Highlight regions with angles greater than 30 degrees
    # threshold_angle_deg = 10
    # highlighted_regions_sma_20 = dollar_bars_data[dollar_bars_data['angle_sma_20'].abs() > threshold_angle_deg]
    # highlighted_regions_sma_30 = dollar_bars_data[dollar_bars_data['angle_sma_30'].abs() > threshold_angle_deg]
    # highlighted_regions_sma_50 = dollar_bars_data[dollar_bars_data['angle_sma_50'].abs() > threshold_angle_deg]
    # highlighted_regions_ema_20 = dollar_bars_data[dollar_bars_data['angle_ema_20'].abs() > threshold_angle_deg]
    # highlighted_regions_ema_30 = dollar_bars_data[dollar_bars_data['angle_ema_30'].abs() > threshold_angle_deg]
    # highlighted_regions_ema_50 = dollar_bars_data[dollar_bars_data['angle_ema_50'].abs() > threshold_angle_deg]



    # Add scatter plot for candlestick
    fig.add_trace(go.Candlestick(x=dollar_bars_data['timestamp'],
                                 open=dollar_bars_data['open'],
                                 high=dollar_bars_data['open'],
                                 low=dollar_bars_data['close'],
                                 close=dollar_bars_data['close'],
                                 increasing_line_color='green',
                                 decreasing_line_color='red',
                                 name='Dollar Bars'), row=1, col=1)

    # Add scatter plots for SMA and EMA
    # SMA traces
    fig.add_trace(go.Scatter(x=dollar_bars_data['timestamp'], y=dollar_bars_data['sma_20'],
                             mode='lines', line=dict(color='blue'), name='SMA 20'), row=1, col=1)
    fig.add_trace(go.Scatter(x=dollar_bars_data['timestamp'], y=dollar_bars_data['sma_30'],
                             mode='lines', line=dict(color='orange', dash='dash'), name='SMA 30'), row=1, col=1)
    fig.add_trace(go.Scatter(x=dollar_bars_data['timestamp'], y=dollar_bars_data['sma_50'],
                             mode='lines', line=dict(color='green', dash='dot'), name='SMA 50'), row=1, col=1)

    # EMA traces
    fig.add_trace(go.Scatter(x=dollar_bars_data['timestamp'], y=dollar_bars_data['ema_20'],
                             mode='lines', line=dict(color='blue'), name='EMA 20'), row=1, col=1)
    fig.add_trace(go.Scatter(x=dollar_bars_data['timestamp'], y=dollar_bars_data['ema_30'],
                             mode='lines', line=dict(color='orange', dash='dash'), name='EMA 30'), row=1, col=1)
    fig.add_trace(go.Scatter(x=dollar_bars_data['timestamp'], y=dollar_bars_data['ema_50'],
                             mode='lines', line=dict(color='green', dash='dot'), name='EMA 50'), row=1, col=1)


    # highlighted_regions_sma_20 = dollar_bars_data[dollar_bars_data['angle_sma_20'].abs() > threshold_angle_deg].index

    # print(f'len: {highlighted_regions_sma_20.shape}')
    # abc = dollar_bars_data['angle_sma_20'].loc[highlighted_regions_sma_20]
    # print(abc)
    # print(abc.shape)

    # # Add shaded regions to the plot
    # for region in highlighted_regions_sma_20[:-1]:  # Exclude the last region
    #     x0_val = dollar_bars_data['timestamp'].loc[region]
    #     # x1_val = dollar_bars_data['timestamp'].loc[region + 1]

    #     # Ensure that region + 1 is within the bounds of the DataFrame
    #     if region + 1 < len(dollar_bars_data):
    #         x1_val = dollar_bars_data['timestamp'].iloc[region + 1]
    #     else:
    #         # Handle the case where region + 1 is out of bounds
    #         x1_val = dollar_bars_data['timestamp'].iloc[-1]  # Set x1_val to the last timestamp in the DataFrame

    #     fig.add_shape(
    #         type="rect",
    #         x0=x0_val,
    #         x1=x1_val,
    #         y0=dollar_bars_data['price'].min(),
    #         y1=dollar_bars_data['price'].max(),
    #         fillcolor="lightcoral",
    #         opacity=0.5,
    #         layer="below",
    #     )

    # Add inverse_time_momentum plot
    # fig.add_trace(go.Scatter(x=dollar_bars_data['timestamp'], y=dollar_bars_data['inverse_time_momentum'],
    #                          mode='lines', line=dict(color='blue'), name='Inverse Time Momentum'),
    #               row=2, col=1)

    # Add time_diff plot
    fig.add_trace(go.Scatter(x=dollar_bars_data['timestamp'], y=dollar_bars_data['time_diff'],
                             mode='lines', line=dict(color='blue'), name='Time Diff'),
                  row=2, col=1)


    # # Update layout
    # fig.update_layout(title_text='Dollar Bars Candlestick Chart and Inverse Time Momentum',
    #                   xaxis=dict(title='Timestamp'),
    #                   width=width,
    #                   height=height)

    # Update layout to share x-axis
    fig.update_layout(
                      width=width,
                      height=height
                      )

    fig.update_xaxes(rangeslider_visible=True, row=1, col=1, rangeslider_thickness=0.01)
    fig.update_xaxes(rangeslider_visible=True, row=2, col=1, rangeslider_thickness=0.01)
    # fig.update_xaxes(rangeslider_visible=True, row=3, col=1)

    # fig.update_xaxes(rangeslider_visible=True, rangeslider_thickness=0.1)

    if save_path:
        fig.write_html(save_path, full_html=False)
    else:
        fig.show()


if __name__ == '__main__':


    # Example usage: Fetch 100 recent trades for the BTC/USDT trading pair
    # symbol = 'MATIC/USDT'
    # limit = 10000000000

    # Fetch tick data
    # tick_data = fetch_binance_tick_data(symbol, limit)
    tick_data = fetch_binance_tick_data_from_csv()
    # tick_data = tick_data[:2000000]
    # Generate dollar bars with a target dollar value of 100,000
    target_dollar_value = 10000000
    dollar_bars = generate_dollar_bars(tick_data, target_dollar_value)

    dollar_bars = calculate_time_based_momentum(dollar_bars)
    # print(dollar_bars)
    dollar_bars = add_sma_ema_to_dataframe(dollar_bars)

    # exit()
    # Plot and save the generated dollar bars as a candlestick chart
    save_path = 'a.html'
    if os.path.exists(save_path):
        os.unlink(save_path)

    plot_dollar_bars_candlestick(dollar_bars, save_path=save_path, width=1920, height=1080)
