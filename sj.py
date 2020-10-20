import requests
from sqlalchemy import create_engine, Table, Column, String, Float, MetaData
from sqlalchemy.sql import select
import json

url = 'http://api.worldweatheronline.com/premium/v1/past-weather.ashx'
params = {
    'key': 'e7506da2237c4758bfb91907202010',
    'date': '2020-09-20',
    'enddate': '2020-09-20',
    'q': 'Volgograd',
    'format': 'json',
}
req = requests.get(url, params).json()
with open('mydata.json', 'w') as f:
    json.dump(req, f)
