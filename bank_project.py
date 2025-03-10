from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import requests
import sqlite3




url="https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs=["Name","MC_USD_BILLION"]
table_attribs_final=["Name","MC_USD_Billion","MC_EUR_Billion","MC_GBP_Billion","MC_INR_Billion"]
connection=sqlite3.connect('Banks.db')
table_name="Largest_banks"




progfile="progfile.txt"
largest_bank_data="trans_data.csv"

def log_progress(message):
 time_format='%d-%h-%Y-%H:%M:%S'
 now=datetime.now()
 timestamp=now.strftime(time_format)
 with open(progfile,"a") as f:
  f.write("Operation Performed at " + timestamp + " " +message+ "\n")

  
  



log_progress("Log process Started")




# Code for ETL operations on Country-GDP data

# Importing the required libraries


def extract(url, table_attribs):

   table=pd.read_html(url)
   df=table[1]
   df=df.iloc[:,1:3]
   df.columns=[table_attribs]
  #  print(df)
   return df

def transform(df, csv_path):
    with open(csv_path,'r') as f:
       exc_rate=f.read()
    dict={}
    lines=exc_rate.strip().split("\n")
    headers=lines[0].split(",")
    for line in lines[1:]:
       values=line.split(",")
       dict[values[0]]=values[1]
    for curr,rates in dict.items():
       df[curr]=round(df["MC_USD_BILLION"]*float(rates),2)
    
    df.columns=[table_attribs_final]



    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
   df.to_sql(table_name,sql_connection,if_exists='replace',index=False)
   


def run_query(query_statement, sql_connection):
    query_output = pd.read_sql(query_statement,sql_connection)

    print(query_statement)
    print(query_output)

log_progress("Extraction Started")
extracted_dataframe=extract(url,table_attribs)
log_progress("Extraction Ended")

log_progress("Transformation Started")
trans_data=transform(extracted_dataframe,"/Users/anupamchaudhary/Desktop/Python/dataeng/exchange_rate.csv")
trans_data.columns=[table_attribs_final]
log_progress("Transformation Ended")

log_progress("Loading to csv")
load_to_csv(trans_data,largest_bank_data)
log_progress("Loading to csv completed")

log_progress("Loading to SQL")
load_to_db(trans_data,connection,table_name)
log_progress("Loading to SQL completed")

query_statement = F"""SELECT "('Name',)", "('MC_EUR_Billion',)" FROM Largest_banks"""

run_query(query_statement,connection)

# print(trans_data.columns)