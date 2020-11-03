import requests
from sqlalchemy import create_engine, Table, Column, String, MetaData, Integer
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
        return [
            {
                'date': row['date'][:10],
                'mint': row['mintempC'],
                'maxt': row['maxtempC'],
                'location': 'Volgograd, Russia',
                'humidity': row['hourly'][0]['humidity'],
            }
            for row in data['data']['weather']
        ]

class WorkWithDB:
    def __init__(self, engine_name):
        self.engine_name = engine_name

    def create_table(self):
        engine = create_engine(self.engine_name)
        metadata = MetaData()
        new_weather = Table(
            'weather',
            metadata,
            Column('date', String),
            Column('mint', Integer),
            Column('maxt', Integer),
            Column('location', String),
            Column('humidity', Integer),
        )
        return new_weather, engine, metadata

    def show(self, db_connect, weather):
        for row in db_connect.execute(select([weather])):
            print(row)

