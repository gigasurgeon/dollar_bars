import pandas as pd
from lightweight_charts import Chart

if __name__ == '__main__':   
		raw_df = pd.read_json("intraday.json", lines = True)
		df = pd.DataFrame({
				"time" : pd.to_datetime(raw_df.TradingDate + ' ' + raw_df.Time, format = "%d/%m/%Y %H:%M:%S"),
				"open" : raw_df.Open,
				"high" : raw_df.High,
				"low" : raw_df.Low,
				"close" : raw_df.Close,
				"volume" : raw_df.LastVol
			})
		chart = Chart(volume_enabled=False)
		chart.set(df)
		chart.show(block=True)
