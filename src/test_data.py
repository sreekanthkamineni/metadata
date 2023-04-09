import sqlite3

conn = sqlite3.connect('weater.db')
cur = conn.cursor()

# pass the table name to check the date
# availble tbales DAILY_TEMP for all data
# monthly max temp : MONTHLY_TEMP
# daily aggrigate table : daily_agg
table_name = 'daily_agg'
sql_query_get = "select * from {}".format(table_name)
table_data = cur.execute(sql_query_get)
for k in table_data:
    print(k)
