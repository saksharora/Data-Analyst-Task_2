import yfinance as yf
from datetime import datetime, timedelta
import pytz
import pandas as pd
from pymongo import MongoClient
import APScheduler.scheduler

# Set the time zone for the data
tz = pytz.timezone('Asia/Kolkata')

# Define the start and end times for the data
now = datetime.now(tz)
end_time = now.replace(second=0, microsecond=0)
start_time = end_time - timedelta(days=7)

# Define a custom time interval for the data
interval = '60m'

# Define the opening and closing times for the custom interval
open_time_1 = timedelta(minutes=15)
close_time_1 = timedelta(minutes=45)
open_time_2 = timedelta(minutes=45)
close_time_2 = timedelta(minutes=15)

# Define a custom function to adjust the timestamps for the custom interval
def adjust_timestamp(timestamp):
 if timestamp.minute < 15:
  return timestamp.replace(minute=15, second=0, microsecond=0)
 elif timestamp.minute < 45:
  return timestamp.replace(minute=45, second=0, microsecond=0)
 else:
  return timestamp + timedelta(hours=1)

# Define a function to download the data from Yahoo Finance
def download_data():
    # Download the data with the custom time interval and adjust the timestamps
    data = yf.download('ICICIBANK.NS', start=start_time, end=end_time, interval=interval)
    data = data.rename_axis('Date_Time').reset_index()
    data['Date_Time'] = data['Date_Time'].apply(adjust_timestamp)
    data = data.set_index('Date_Time')

    return data

# Define a function to store the data in the MongoDB database
def store_data(data):
    # Connect to the MongoDB database
    client = MongoClient('localhost', 27017)
    db = client['my_database']
    collection = db['icici_bank_data']

    # Insert the data into the MongoDB collection
    for row in data.iterrows():
       collection.insert_one(row[1].to_dict())

    # Close the connection to the MongoDB database
    client.close()

# Schedule the program to run every 15 minutes from 11.15 AM to 2.15 PM daily
scheduler = APScheduler.scheduler.Scheduler()

@scheduler.interval_schedule(minutes=15, start_date='2023-10-24 11:15:00', end_date='2023-10-24 14:15:00')
def store_data():
    # Download the data from Yahoo Finance
    data = download_data()

    # Store the data in the MongoDB database
    store_data(data)

scheduler.start()

# Keep the program running forever
while True:
    pass
