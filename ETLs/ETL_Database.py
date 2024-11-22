#=================================== Data Extracting from DB2 server ============================================
import sqlite3
import pandas as pd 
conn = sqlite3.connect('STAFF.db')


table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

#Reading CSV file
file_path = '/home/project/INSTRUCTOR.csv'
df = pd.read_csv(file_path, names = attribute_list)

#Loading Data to the Table
df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')

# Viewing all the data in table
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#viewing only Fname col and data
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#viewing total number of entries
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#creating data frame
data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)

conn.close()
