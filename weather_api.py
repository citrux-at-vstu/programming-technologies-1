import requests
from sqlalchemy import create_engine, Table, Column, String, Integer, MetaData
from sqlalchemy.sql import select


class WeatherProvider:
    def __init__(self, key):
        self.key = key

    def get_data(self, location, start_date, end_date, prov_url):
        params = {
            'tp':  24,
            'date': f'{start_date}',
            'enddate': f'{end_date}',
            'q': location,
            'key': self.key,
            'format': 'json',
        }
        data = requests.get(prov_url, params).json()
        # return [
        #     {
        #         'date': row['datetimeStr'][:10],
        #         'mint': row['mint'],
        #         'maxt': row['maxt'],
        #         'location': 'Volgograd,Russia',
        #         'humidity': row['humidity'],
        #     }
        #     for row in data['locations'][location]['values']
        # ]
        return [
            {
                'date': row['date'],
                'mintempC': row['mintempC'],
                'maxtempC': row['maxtempC'],
                'location': 'Volgograd, Russia',
                'humidity': row['humidity']
            }
            for row in data['date']['city']['value']
        ]

class WorkWithDB:
    def __init__(self, engine_name):
        self.engine_name = engine_name

    def create_table(self):
        engine = create_engine(self.engine_name)
        metadata = MetaData()
        weather = Table(
            'weather',
            metadata,
            Column('date', String),
            Column('mint', Integer),
            Column('maxt', Integer),
            Column('location', String),
            Column('humidity', Integer),
        )
        return weather, engine, metadata

    def show(self, db_connect, weather):
        for row in db_connect.execute(select([weather])):
            print(row)

new_db = WorkWithDB('sqlite:///weather.sqlite3')
weather, engine, metadata = new_db.create_table()
metadata.create_all(engine)
db_connect = engine.connect()
provider = WeatherProvider('e7506da2237c4758bfb91907202010')
db_connect.execute(weather.insert(), provider.get_data('Volgograd', '2020-09-20', '2020-09-29',
                                                       'http://api.worldweatheronline.com/premium/v1/past-weather.ashx'))
new_db.show(db_connect, weather)