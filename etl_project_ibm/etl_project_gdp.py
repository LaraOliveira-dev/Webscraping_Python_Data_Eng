"""
An international firm that is looking to expand its business in different countries across the world has recruited you. 
You have been hired as a junior Data Engineer and are tasked with creating an automated script that can extract the list of all countries 
in order of their GDPs in billion USDs (rounded to 2 decimal places), as logged by the International Monetary Fund (IMF). 
Since IMF releases this evaluation twice a year, this code will be used by the organization to extract the information as it is updated.

You can find the required data on this webpage: 
https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29

The required information needs to be made accessible as a JSON file 'Countries_by_GDP.json' as well as a table 'Countries_by_GDP' 
in a database file 'World_Economies.db' with attributes 'Country' and 'GDP_USD_billion.'

Your boss wants you to demonstrate the success of this code by running a query on the database table to display only the entries 
with more than a 100 billion USD economy. Also, log the entire process of execution in a file named 'etl_project_log.txt'.
You must create a Python code 'etl_project_gdp.py' that performs all the required tasks."""

# Code for ETL operations on Country-GDP data
# Importing the required libraries

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'

def extract(url, table_attribs):
    ''' The purpose of this function is to extract the required
    information from the website and save it to a dataframe. The
    function returns the dataframe for further processing. '''

    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]: # if column[0] has a and a is not empty and '-' is not in column[2] 
                data_dict = {"Country": col[0].a.contents[0], # create the dictionary with required info
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0]) # generate dataframe from the dictionary
                df = pd.concat([df,df1], ignore_index=True) # concatenate the dataframes (original empty one and the new one)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    GDP_list = df["GDP_USD_millions"].tolist() # get the list of GDP values from the dataframe column GDP_USD_millions
    GDP_list = [float("".join(x.split(','))) for x in GDP_list] # convert the currency format (string) to float 
    GDP_list = [np.round(x/1000,2) for x in GDP_list] # convert from millions to billions and round to 2 decimal places
    df["GDP_USD_millions"] = GDP_list # update the dataframe column with the new list
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"}) # rename the column to reflect the new unit
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path)

def load_to_json(df, json_path):
    ''' This function saves the final dataframe as a `JSON` file 
    in the provided path. Function returns nothing.'''

    df.to_json(json_path, orient='records', lines=True) 

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe to as a database table
    with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection) # run the query and save the output to a dataframe
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) # format the timestamp
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')    

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

load_to_json(df, 'Countries_by_GDP.json')

log_progress('Data saved to JSON file')

sql_connection = sqlite3.connect('World_Economies.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100" # define the query statement
run_query(query_statement, sql_connection) # run the query on the database

log_progress('Process Complete.')

sql_connection.close()