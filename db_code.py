import pandas as pd
import sqlite3
conn=sqlite3.connect('Staff.db')

attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']
table_name="europe"

df=pd.read_csv('INSTRUCTOR.csv',names=attribute_list)
print(df)

df.to_sql(table_name,conn,if_exists='replace',index=False)
query_statement = f"SELECT CITY FROM {table_name}"
query_output = pd.read_sql(query_statement,conn)
print(query_statement)
print(query_output)