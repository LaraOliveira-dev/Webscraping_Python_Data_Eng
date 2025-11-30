"""
Final project overview
Estimated effort: 5 mins
In this project, you will put all the skills acquired throughout the course and your 
knowledge of basic Python to test. You will work on real-world data and perform the operations 
of Extraction, Transformation, and Loading as required. Throughout the project, 
you will note some outputs you need to answer questions on the graded quiz. You will also take 
snapshots, which you will upload in the peer-graded assignment.

Project Scenario
A multi-national firm has hired you as a data engineer. Your job is to access and process data 
as per requirements.

Your boss asked you to compile the list of the top 10 largest banks in the world ranked by 
market capitalization in billion USD. Further, you need to transform the data and store it 
in USD, GBP, EUR, and INR per the exchange rate information made available to you as a CSV file. 
You should save the processed information table locally in a CSV format and as a database table. 
Managers from different countries will query the database table to extract the list and 
note the market capitalization value in their own currency.

Directions
1) Write a function to extract the tabular information from the given URL under the heading By 
Market Capitalization, and save it to a data frame.
2) Write a function to transform the data frame by adding columns for Market Capitalization in GBP, EUR, and INR, rounded to 2 decimal places, 
based on the exchange rate information shared as a CSV file.
3) Write a function to load the transformed data frame to an output CSV file.
4) Write a function to load the transformed data frame to an SQL database server as a table.
5) Write a function to run queries on the database table.

Run the following queries on the database table:
a. Extract the information for the London office, that is Name and MC_GBP_Billion
b. Extract the information for the Berlin office, that is Name and MC_EUR_Billion
c. Extract the information for New Delhi office, that is Name and MC_INR_Billion

6) Write a function to log the progress of the code.
7) While executing the data initialization commands and function calls, maintain appropriate 
log entries.
"""

# Code for ETL operations on Bank Market Capitalization data

import requests
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_extraction_attribs = ["Name", "MC_USD_Billion"]
table_final_attribs = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
csv_path = "Largest_Banks_by_data.csv"
db_name = "Banks.db"
table_name = "Largest_Banks"
log_file = "code_log.txt"

"""1) Write a function to extract the tabular information from the given URL under the heading By 
Market Capitalization, and save it to a data frame."""

def extract(url, table_extraction_attribs):
    ''' This function extracts the required tabular information
    from the given URL and saves it to a dataframe. The function
    returns the dataframe for further processing. '''
    
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_extraction_attribs)
    
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            # catch all <a> tags in the Name column and get the last one with the bank name
            a_tags = col[1].find_all('a')
            if a_tags:
                name_tag = a_tags[-1]  #the last <a> tag is the one with the bank name
                name = name_tag.get_text(strip=True)
                mc_text = col[2].get_text(strip=True)
                
                if name and mc_text and mc_text != '—':
                    data_dict = {"Name": name,
                                "MC_USD_Billion": mc_text}
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df1], ignore_index=True)

    return df

"""2) Write a function to transform the data frame by adding columns for Market Capitalization 
in GBP, EUR, and INR, rounded to 2 decimal places, 
based on the exchange rate information shared as a CSV file."""

def transform(df):
    with open('./exchange_rate.csv', 'r') as currency:
        rates = pd.read_csv(currency)
    
    new_tables = pd.DataFrame(columns=table_final_attribs)
    for row in df.iterrows(): # iterate over dataframe rows
        data = row[1] 

        values_df = data["MC_USD_Billion"]  
        value = float(values_df) 
        
        convert_to_GBP = rates.loc[rates['Currency'] == 'GBP', 'Rate'].values[0]
        convert_to_INR = rates.loc[rates['Currency'] == 'INR', 'Rate'].values[0]
        convert_to_EUR = rates.loc[rates['Currency'] == 'EUR', 'Rate'].values[0]

        mc_gbp = round(convert_to_GBP * value, 2)
        mc_eur = round(convert_to_EUR * value, 2)
        mc_inr = round(convert_to_INR * value, 2)

        data_dict = {
            "Name": data["Name"],
            "MC_USD_Billion": value,
            "MC_GBP_Billion": mc_gbp,
            "MC_EUR_Billion": mc_eur,
            "MC_INR_Billion": mc_inr
        }
        
        df1 = pd.DataFrame([data_dict])
        new_tables = pd.concat([new_tables, df1], ignore_index=True)
    
    return new_tables

"""3) Write a function to load the transformed data frame to an output CSV file.
"""

def convert_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)
    
"""4) Write a function to load the transformed data frame to an SQL database server as a table.
"""

def convert_to_db(df, db_name, table_name):
    df.to_sql(table_name, sqlite3.connect(db_name), if_exists='replace', index=False)


"""5) Write a function to run queries on the database table.
"""

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection) # run the query and save the output to a dataframe
    print(query_output)

"""6) Write a function to log the progress of the code."""

def log_progress(message):
    with open(log_file, 'a') as log:
        log.write(message + '\n')

"""7) While executing the data initialization commands and function calls, maintain appropriate 
log entries."""

"""Run the following queries on the database table:
a. Extract the information for the London office, that is Name and MC_GBP_Billion
b. Extract the information for the Berlin office, that is Name and MC_EUR_Billion
c. Extract the information for New Delhi office, that is Name and MC_INR_Billion"""

log_progress("Starting ETL process for Largest Banks data. Extracting data")
df = extract(url, table_extraction_attribs)
log_progress("Data extraction complete. Initiating data transformation.")
df = transform(df)
log_progress("Data transformation complete. Initiating data load to CSV.")
convert_to_csv(df, csv_path)
log_progress("Data load to CSV complete.")

sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated.")
convert_to_db(df, db_name, table_name)
log_progress("Data load to Database complete. Initiating queries.")
query_statement_1 = "SELECT Name, MC_GBP_Billion FROM Largest_Banks"
run_query(query_statement_1, sql_connection)
log_progress("Query for London office executed. Starting next query.")
query_statement_2 = "SELECT Name, MC_EUR_Billion FROM Largest_Banks"
run_query(query_statement_2, sql_connection)
log_progress("Query for Berlin office executed. Starting next query")
query_statement_3 = "SELECT Name, MC_INR_Billion FROM Largest_Banks"    
run_query(query_statement_3, sql_connection)
log_progress("Query for New Delhi office executed.")
log_progress("Queries executed. ETL process complete.")
sql_connection.close()