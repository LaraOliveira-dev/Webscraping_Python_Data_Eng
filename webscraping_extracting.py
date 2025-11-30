import sqlite3
import pandas as pd
from bs4 import BeautifulSoup # BeautifulSoup for HTML parsing
import requests

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
csv_path = 'top_50_films.csv'
df = pd.DataFrame(columns=["Average Rank","Film","Year"])
count = 0

html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')
print("HTML carregado com sucesso!")

tables = data.find_all('tbody') # Get all table bodies
rows = tables[0].find_all('tr') # Get all rows from the first table body

for row in rows:
    if count<50:
        col = row.find_all('td')
        
        if len(col)!=0:
            year_value = int(col[2].get_text().strip())
            data_dict = {"Average Rank": col[0].contents[0],
                         "Film": col[1].contents[0],
                         "Year": year_value}
            df1 = pd.DataFrame(data_dict, index=[0])
            df1_updated = df1.loc[df1["Year"] >= 2000, ["Film"]] # Filter films from year 2000 onwards
            df = pd.concat([df,df1_updated], ignore_index=True) # Append new row to dataframe
            count+=1
    else:
        break

print(df)

# Save to CSV and SQL database

df.to_csv(csv_path)
conn = sqlite3.connect(db_name) # Open connection to SQLite database
df.to_sql(table_name, conn, if_exists='replace', index=False) # Save dataframe to SQL table
conn.close() # Close the connection

# This code update the CSV and SQL database with the top 50 films from the specified webpage, the db file is Movies.db and the CSV table is Top_50. 