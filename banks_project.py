# Code for ETL operations on Country-GDP data

# Importing the required libraries
import os
import requests
import numpy as np
import pandas as pd
import sqlite3
from lxml import etree
from bs4 import BeautifulSoup

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    filename = "code_log.txt"
    with open(filename, "a") as file:
        file.write(f"{message}\n")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    df = None
    xpath = '//div[descendant::h2/span[contains(text(), "By market capitalization")]]//table[.//th[contains(text(), "Market cap")]]'
    response = requests.get(url)
    content = response.text

    soup = BeautifulSoup(content, 'html.parser')
    dom = etree.HTML(str(soup))

    table = dom.xpath(xpath)

    if not table:
        log_progress("Critical - Unable to fetch the table using xpath")
        # return None    
    
    if len(table) > 1:
        log_progress("Warning - Found Multiple Tables")
    # selecting the first table
    table = table[0]

    rows = table.xpath('.//tr')
    if len(rows) < 1:
        log_progress("Critical - Failed to select tr")
        # return None

    headings = rows.pop(0)
    column_names = headings.xpath('./th')
    if not column_names:
        log_progress("Warning - Failed to get column names from th tag")

    else:
        column_names = [name.text.strip() for name in column_names]
        df = pd.DataFrame(columns=column_names)

    for row in rows:
        cell = row.xpath('.//td')
        if len(cell) != len(df.columns):
            log_progress("Error - Row and Columns Length Mismatch - Moving to next ")
            continue
        
        cell_values = []
        cell_values.append(cell[0].text.strip())
        cell_values.append((cell[1].xpath('./a'))[0].text.strip())
        cell_values.append(cell[2].text.strip())
        df.loc[len(df)]= cell_values

    log_progress("Data extraction complete. Initiating Transformation process")
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    csv_df = pd.read_csv(csv_path)
    csv_df_dict = dict(zip(csv_df.Currency, csv_df.Rate))
    print(csv_df_dict)

    df.insert(len(df.columns), column="MC_GBP_Billion", value=None)
    df.insert(len(df.columns), column="MC_EUR_Billion", value=None)
    df.insert(len(df.columns), column="MC_INR_Billion", value=None)

    df['MC_GBP_Billion'] = [np.round(float(value) * csv_df_dict['GBP'], 2) for value in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(float(value) * csv_df_dict['EUR'], 2) for value in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(float(value) * csv_df_dict['INR'], 2) for value in df['MC_USD_Billion']]

    log_progress("Data transformation complete. Initiating Loading process")
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    try:
        df.to_csv(output_path, index=False, encoding="utf-8")
        log_progress("Data saved to CSV file")

    except Exception as e:
        log_progress(f"Failed to load data to csv with an error {e}")

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    try:
        df.to_sql(
            table_name,
            sql_connection,
            if_exists="replace"
        )
        log_progress('Data loaded to Database as a table, Executing queries')
    except pd.errors.DatabaseError as e:
        log_progress(f"Error - Loading df to db failed with an error {e}")

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    try:
        result = pd.read_sql_query(query_statement, sql_connection)
        print(result)
        log_progress('Process Complete')
    except pd.errors.DatabaseError as e:
        log_progress(f"Error - Query failed to execute with an error {e}")

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

df = extract("https://en.wikipedia.org/wiki/List_of_largest_banks", None)
df = df.rename(columns={'Bank name': 'Name', 'Market cap': 'MC_USD_Billion'}).drop(['Rank'], axis=1)
print(df)
transformed_df = transform(df, "./exchange_rate.csv")
print()
print(transformed_df)
print()
print(transformed_df['MC_EUR_Billion'][4])
output_filename = "Largest_banks_data.csv"
load_to_csv(transformed_df, os.path.join(os.path.abspath(""), output_filename))
table_name = "Largest_banks"
db_name = "Banks.db"
conn = sqlite3.connect(table_name)
log_progress('Server Connection initiated')
load_to_db(transformed_df, conn, table_name)
query ='''SELECT * FROM Largest_banks'''
run_query(query_statement=query, sql_connection=conn)
query ='''SELECT AVG(MC_GBP_Billion) FROM Largest_banks'''
run_query(query_statement=query, sql_connection=conn)
query ='''SELECT Name from Largest_banks LIMIT 5'''
run_query(query_statement=query, sql_connection=conn)
conn.close()
log_progress('Server Connection closed')
