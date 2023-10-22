import yfinance as yf
from datetime import datetime, timedelta
import pytz
import pandas as pd
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

tz = pytz.timezone('Asia/Kolkata')
now = datetime.now(tz)
end_time = now.replace(second=0, microsecond=0)
start_time = end_time - timedelta(days=7)

interval = '60m'

open_time_1 = timedelta(minutes=15)
close_time_1 = timedelta(minutes=45)
open_time_2 = timedelta(minutes=45)
close_time_2 = timedelta(minutes=15)

def adjust_timestamp(timestamp):
    if timestamp.minute < 15:
        return timestamp.replace(minute=15, second=0, microsecond=0)
    elif timestamp.minute < 45:
        return timestamp.replace(minute=45, second=0, microsecond=0)
    else:
        return timestamp + timedelta(hours=1)

def download_data():
    data = yf.download('ICICIBANK.NS', start=start_time, end=end_time, interval=interval)
    data = data.rename_axis('Date_Time').reset_index()
    data['Date_Time'] = data['Date_Time'].apply(adjust_timestamp)
    data = data.set_index('Date_Time')
    return data

def store_data(data):
    client = MongoClient('localhost', 27017)
    db = client['my_database']
    collection = db['icici_bank_data']
    for row in data.iterrows():
        collection.insert_one(row[1].to_dict())
    client.close()

my_scheduler = BackgroundScheduler()

@my_scheduler.scheduled_job(trigger=IntervalTrigger(minutes=15), start_date='2023-10-24 11:15:00', end_date='2023-10-24 14:15:00')
def download_and_store_data():
    data = download_data()
    store_data(data)

my_scheduler.start()
