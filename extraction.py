from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from dotenv import dotenv_values

def table1_extract():
    # this function extracts html data from a webpage and stores it into a raw folder. It detokenizes the extracted data 
    # and puts it in a pandas dataframe structure.
    url = 'https://afx.kwayisi.org/ngx/'                 # URL containing the script
    response = requests.get(url)                         # Placing a request for the script

    try:
        if response.status_code == 200:
            print('Table 1 request successful')
        elif response.status_code == 404:
            print('Bad request error')
        elif response.status_code == 401:
            print('Recheck connection')
        else:
            print('Unknown error')

        content_reveal = response.content                # Parse the content of the requested url
        soup = BeautifulSoup(content_reveal, 'lxml')    # BS4 to parse html data into selectable elements 
        tables = soup.find_all('table')                 # Find all <table> elements 
        first_table = tables[3]                         #if tables else None # Select the needed table element. It is located in table 3.
        
        try:
            if first_table:  
                print('Table 1 found')
                stringified = str(first_table)              # convert the table into a string format
                in_stringIO = StringIO(stringified)         # Then Wrap selected_table html string with stringIO object where it is read as a file
                df = pd.read_html(in_stringIO)              # Then read the stringified and wrapped html content table into dataframes
                df_select = df[0]                           # select the first dataframe
                print('Table 1 dataframe saved successfully')
                return df_select
        except:   
            print('Table 1 not found')
    except AttributeError:
        print('AttributeError: The response object is missing the status_code attribute')
    except Exception as e:
        print('An unexpected error occurred:', e)   

def table2_extract():
    url_2 = 'https://afx.kwayisi.org/ngx/?page=2'                       # second html data
    response = requests.get(url_2)                                      # request for second html data
    
    try:
        if response.status_code == 200:
            print('Table 2 Request successful')
        elif response.status_code == 404:
            print('Bad request error')
        elif response.status_code == 401:
            print('Recheck connection')
        else:
            print('Unknown error')

        showURLcontent = response.content                               # access the raw binary representation of the response 
        soup2 = BeautifulSoup(showURLcontent, 'lxml')                   # to extract specific elements, search for tags, and extract data from the parsed document.
        all_tables = soup2.find_all('table')                            # searches for all occurrences of that tag in the document represented by soup2.
        table_2 = all_tables[3]
        try: 
            if table_2:
                print('Table 2 found')
                string_pref_table = str(table_2)
                IO_stringed_table = StringIO(string_pref_table)
                df2 = pd.read_html(IO_stringed_table)
                df2_select = df2[0]
                print('Table 2 dataframe saved successfully')
                return df2_select
        except:
            print('Table 2 not found')
    except AttributeError:
        print('AttributeError: The response object is missing the status_code attribute')
    except Exception as e:
        print('An unexpected error occurred:', e)   
     
def concat_tables():   
    joint_dfs = pd.concat([table1_extract(), table2_extract()], axis=0)
    print('joint dataframes created successfully')

    # Adding date column to the raw file because it has no scrapped date
    joint_dfs['date']
    currDate = datetime.now()

    formatted_currDate = currDate.strftime("%Y-%m-%d")
    joint_dfs.to_csv(f'raw/record_{formatted_currDate}.csv', index=False)
    print('joint dataframes saved as csv in raw folder successfully')
    new_csv = pd.read_csv(f'raw/record_{formatted_currDate}.csv')
    return new_csv
    
def transformed():
    imp_csv = concat_tables()
    print('csv file opened')
    imp_csv['date'] = datetime.now().date()                            # adding current date column to csv
    print('current date column included successfully')
    null_vol = imp_csv['Volume'].isna()                                # all empty volume fields of the volume column
    mean_volume = imp_csv['Volume'].mean()
    imp_csv.loc[null_vol, 'Volume'] = mean_volume
    currDate = datetime.now()
    formatted_currDate = currDate.strftime("%Y-%m-%d")
    final_csv = imp_csv.to_csv(f'transformed/record_{formatted_currDate}.csv', index=False)
    from_transformed = pd.read_csv(final_csv)
    return from_transformed
        
def db_config():
    config = dotenv_values(".env")
    dbname = config['dbname']
    host = config['host']
    port = config['port']
    password = config['password']
    username = config['username']
    return create_engine(f'postgresql+psycopg2://{dbname}:{port}@{host}:{password}:{username}')

def csv_to_postgres():   
    csv_load = transformed()
    table_name = "jazz"                                   
    csv_load.to_sql(table_name, con=db_config(), if_exists='replace', index=False)
    print('File_Sent_To_Postgres_Database')

concat_tables()
transformed()
csv_to_postgres()