
import database_tables as database_tables
import api_data_extract as api_data_extract
import pandas as pd
from datetime import datetime, timedelta


locations = [
    {'name': 'London', 'lat': '51.5072', 'lon': '-0.1276'},
    {'name': 'New York', 'lat': '40.7128', 'lon': '-74.0060'},
    {'name': 'Ottawa', 'lat': '45.41666', 'lon': '-75.700'},
    {'name': 'New Delhi', 'lat': '28.6', 'lon': '77.2000'},
    {'name': 'Rome', 'lat': '41.9', 'lon': '12.483333'},
    {'name': 'Wellington', 'lat': '-41.3', 'lon': '174.783333'},
    {'name': 'Doha', 'lat': '25.283333', 'lon': '51.533333'},
    {'name': 'Moscow', 'lat': '55.75', 'lon': '37.600000'},
    {'name': 'Beijing', 'lat': '39.9166', 'lon': '116.383333'},
    {'name': 'Brasilia', 'lat': '-15.7833333', 'lon': '-47.916667'},
]


def month_data_extract(DAILY_TEMP_data_df):
    #    print(DAILY_TEMP_data_df)
    df = pd.DataFrame(DAILY_TEMP_data_df, columns=['location', 'date', 'temperature'])
    df['year'] = pd.DatetimeIndex(df['date']).year
    df['Month'] = pd.DatetimeIndex(df['date']).month
    result_df = df.sort_values('temperature').groupby(['location', 'year', 'Month']).tail(1)
    return result_df.values


def daily_agg_extract(DAILY_TEMP_data_df):
    df = pd.DataFrame(DAILY_TEMP_data_df, columns=['location', 'date', 'temperature'])
    df['Max_temp'] = df.groupby(['location', 'date'])['temperature'].transform(max)
    df['Min_temp'] = df.groupby(['location', 'date'])['temperature'].transform(min)
    df['Avg_temp'] = df.groupby(['location', 'date'])['temperature'].transform('mean').round(2)
    result_df = df.groupby(['location', 'date']).tail(1)
    return result_df[['location', 'date', 'Max_temp', 'Min_temp', 'Avg_temp']].values


def extract_api_data():
    # Extract data for the last 5 days for all locations
    start_date = datetime.now() - timedelta(days=5)
    end_date = datetime.now()
    data = []
    for location in locations:
        for date in range(int(start_date.timestamp()), int(end_date.timestamp()), 86400):
            data.append(api_data_extract.extract_data_for_location(location, date))
    return data


def create_tables(db_connectin):
    db_connectin.create_tables_daily()
    db_connectin.create_table_month_high()
    db_connectin.create_table_daily_agg()
    print("tables are created/verified")


if __name__ == '__main__':
    data = extract_api_data()
    db_connectin = database_tables.db_connect()
    create_tables(db_connectin)
    db_connectin.insert_data(data)
    DAILY_TEMP_data_df = db_connectin.get_data_df()

    month_data = month_data_extract(DAILY_TEMP_data_df)
    db_connectin.insert_month_data(month_data)

    daily_agg = daily_agg_extract(DAILY_TEMP_data_df)
    db_connectin.insert_daily_agg_data(daily_agg)
