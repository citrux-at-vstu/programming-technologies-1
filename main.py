from weather_api import WeatherProvider, WorkWithDB

new_db = WorkWithDB('sqlite:///weather.sqlite3')
weather, engine, metadata = new_db.create_table()
metadata.create_all(engine)
db_connect = engine.connect()
provider = WeatherProvider('e7506da2237c4758bfb91907202010')
db_connect.execute(weather.insert(), provider.get_data('Volgograd', '2020-09-20', '2020-09-29',
                                                       'http://api.worldweatheronline.com/premium/v1/past-weather.ashx'))
new_db.show(db_connect, weather)