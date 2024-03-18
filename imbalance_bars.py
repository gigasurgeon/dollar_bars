# https://github.com/cltai9145/research/blob/master/Chapter2/mlfinlab/data_structures/imbalance_data_structures.py
# and
# 

num_prev_bars = 3
expected_num_ticks_init = 100000
expected_num_ticks = expected_num_ticks_init
cum_theta = 0
num_ticks = 0
imbalance_array = []
imbalance_bars = []
bar_length_array = []
for row in data.rows:
    #track high,low,close, volume info
    num_ticks += 1
    tick_rule = get_tick_rule(price, prev_price)
    volume_imbalance = tick_rule * row['volume']
    imbalance_array.append(volume_imbalance)
    cum_theta += volume_imbalance
    if len(imbalance_bars) == 0 and len(imbalance_array) >= expected_num_ticks_init:
        expected_imbalance = ewma(imbalance_array, window=expected_num_ticks_init)

    if abs(cum_theta) >= expected_num_ticks * abs(expected_imbalance):
        bar = form_bar(open, high, low, close, volume)
        imbalance_bars.append(bar)
        bar_length_array.append(num_ticks)
        cum_theta, num_ticks = 0, 0
        expected_num_ticks = ewma(bar_lenght_array, window=num_prev_bars)
        expected_imbalance = ewma(imbalance_array, window = num_prev_bars * expected_num_ticks)