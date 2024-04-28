
10ALYTICS ASSIGNMENT
Web Scrapper ETL Project Case-study:
 Jazzy Investment, one of 10Alytics clients is a stockbroking firm that deals in issuance and trading of stocks on behalf of its customers in the Nigeria Stock Exchange. For the analytics team to analyze market trends and place the best bet on company stocks, they require the daily stock exchange data published on here: https://afx.kwayisi.org/ngx/ 
Your task: As one of the students 10Alytics data engineering training program, we require you to build a web scrapper (data pipeline) to Extract, Transform and load the listed companies/securities data from https://afx.kwayisi.org/ngx/  to a Postgresql database. The data required is under the heading Listed companies/securities This data will be consumed by our client (Jazzy Investment) in the database for their stock trading analytics. 

Requirements: 
1. Assuming the data of company stocks listed is updated on the website every day and your pipeline will run once every day, ensure newly extracted data is integrated with existing data in the database each time your pipeline run. 
2. The first page of the website shows only 100 of 157 listings (click on the Next>> button at the bottom of the table) to confirm this. You are expected to get data for the remaining 57 listing and add to the first 100 list. 
3. Your transformation should include a column called Date which shows the date for which the data was scrapped. 
4. For companies that does not have a value for volume traded for a particular day, then the value for volume should be the mean volume of all companies that traded that day. 
Submission: You’re required to submit one python file (e.g. etl.py) on or before Thursday, 12th October, 2023. NB: The file you’re submitting should NOT be in Jupyter Notebook format but a .py file.

# Checking for Duplicates
1) Include scrapped date to the raw file.
2) Tranform and save in csv
3) Files in Transform folder now become existing files
4) Files in raw folder now becomes incoming files
5) Use the check last updated to check if the aximum date of the dates of the raw file is diferent from that of the maximum date of the transform folder
6) If dates are different, append
7) If dates are same, ignore and add a print message 'Dates already exist'

connection = get_database_conn()
# Get new data
new_data = pd.read_csv('transformed/new_log_data.csv')
#date_convert = lambda date_val: datetime.strptime(date_val, '%d%m%Y')                 #
new_data['date'] = pd.to_datetime(new_data['date']).dt.date

# 2020-01-10 
query = '''
select max(date) from log_data   
'''

# # Get last time data was updated into the database
last_updated = pd.read_sql(query, con= connection).values[0][0]
new_data = new_data[new_data['date'] > last_updated]
new_data.to_sql('log_data', con= connection, if_exists = 'append', index = False)
print('Data loaded successfully')


# RAW FOLDER 
The raw folder contains data newly scripted from the website. These files are then transformed and stored in the transformed folder.

# TRANSFORMED FOLDER
The transfored folder contains data which has been transformed. It is a staging layer, or can be referred to as a datalake. It is from the transformed folder that .csv files are loaded to the database.

# MAIN.PY
The main.py is the entry code that runs the entire project.

# EXTRACTION.PY
The extraction.py script extracts data from the url endpoint. Data returned in .html format consists of tokens; namely titles, div, class, table, table data, table row, paragraphs, headers and href.

# UTIL.PY
The util.py file contains helper functions;
a) getting database connection: 
b) 


# 
