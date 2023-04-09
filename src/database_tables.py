import sqlite3
import pandas as pd


class db_connect():
    def __init__(self):
        self.conn = sqlite3.connect('weater.db')
        self.cur = self.conn.cursor()

    def create_tables_daily(self):
        self.cur.execute(
            ''' CREATE TABLE IF NOT EXISTS DAILY_TEMP
            (location text,date text , temperature NUMERIC)
            '''
        )

    def create_table_month_high(self):
        self.cur.execute(
            ''' CREATE TABLE IF NOT EXISTS MONTHLY_TEMP
                (location text,date text , max_temp NUMERIC, year NUMERIC, month NUMERIC,
                CONSTRAINT unique_name UNIQUE (location, month, year) )
            '''
            )

    def create_table_daily_agg(self):
        self.cur.execute(
            ''' CREATE TABLE IF NOT EXISTS daily_agg
                (location text,date text , Max_temp NUMERIC,Min_temp NUMERIC,Avg_temp NUMERIC,
                CONSTRAINT unique_name UNIQUE (location, date) )
            '''
            )

    def insert_data(self, data):
        for k in data:
            location = k["location"]
            date = k["date"]
            temperature = k["temperature"]
            self.cur.execute(
                    """ INSERT INTO DAILY_TEMP VALUES
                        (?,?,?) """, (location, date, temperature)

            )
        print('data loaded in to primary daily table')
        self.conn.commit()

    def insert_daily_agg_data(self, daily_data):
        for data in daily_data:
            self.cur.execute("""
                INSERT INTO daily_agg (location, date, Max_temp, Min_temp, Avg_temp)
                VALUES (?, ?, ?, ?,?)
                ON CONFLICT (location, date) DO UPDATE SET
                    max_temp = CASE
                                    WHEN max_temp < excluded.max_temp THEN excluded.max_temp
                                    ELSE max_temp
                                END;
            """, data)
            self.cur.execute("SELECT * FROM daily_agg WHERE location=? AND date=?", (data[0], data[1]))
            row = self.cur.fetchone()
            if row:
                # If the row exists, update max_temp and min_temp
                self.cur.execute("UPDATE daily_agg SET max_temp=?, min_temp=?, Avg_temp=? WHERE location=? AND date=?", (data[2], data[3], data[4], data[0], data[1]))
            else:
                # If the row does not exist, insert a new row
                self.cur.execute("INSERT INTO daily_agg (location, date, max_temp, min_temp) VALUES (?, ?, ?, ?, ?)", (data))
        print("monthly max temp table is updated")
        self.conn.commit()

    def insert_month_data(self, month_data):
        for data in month_data:
            self.cur.execute("""
                INSERT INTO MONTHLY_TEMP (location,date,max_temp, year,month )
                VALUES (?, ?, ?, ?,?)
                ON CONFLICT (location, month, year) DO UPDATE SET
                    max_temp = CASE
                                    WHEN max_temp < excluded.max_temp THEN excluded.max_temp
                                    ELSE max_temp
                                END;
            """, data)
        self.conn.commit()
        print("daily aggrigated table updated")

    def get_data_df(self):
        DAILY_TEMP_data = pd.read_sql_query("""
                     SELECT * FROM  DAILY_TEMP""", self.conn)
        return DAILY_TEMP_data

    def get_tabl_data(self, table_name):
        sql_query_get = "select * from {}".format(table_name)
        daily_agg_data = self.cur.execute(sql_query_get)
        return daily_agg_data

    def delete_table_data(self, table_name):
        sql_query_delete = f" DELETE FROM  {table_name}"
        self.cur.execute(sql_query_delete)

    def drop_table(self, table_name):
        sql_query_drop = f" DROP TABLE {table_name}"
        self.cur.execute(sql_query_drop)
