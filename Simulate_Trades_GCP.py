# Define Trades data and GCP credentials
trades_raw = "Trades.csv"
project_id = "cloud-dqm"
topic = "Receive_Trades"

# Import Class
from Publish_Trades_GCP import PublishTrades

# Instatite the Class
main = PublishTrades(project_id, topic, trades_raw)


# Stream Trades to PubSub
main.streamTrades()
