import os
import stat
import requests
import zipfile
import os
import psycopg2
from psycopg2 import OperationalError
import pandas as pd



# Creating dir for files
absPath = os.path.abspath(".")
os.mkdir(f'{absPath}/extracted')
# Setting permision or throws Exception -> 'permission denied'
os.chmod(f"{absPath}/extracted", stat.S_IWUSR)





# URL for 2023 q4
url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/2023q4.zip' 
file_path = './extracted/file.zip'

# SEC headers Requirement: MUST DECLARE USER-AGENT
# User-Agent: Sample Company Name AdminContact@<sample company domain>.com
headers={"User-Agent": "<enter-values-here>"}

response = requests.get(url, headers = headers)
if response.status_code == 200:
    with open(file_path, 'wb') as file:
        file.write(response.content)
else:
    print("Failed to download the file")

# Extract downloaded Zip File
with zipfile.ZipFile(file_path, 'r') as zip_ref:
    zip_ref.extractall("./extracted")

# Deleting as zip file no longer necessary
os.remove("./extracted/file.zip")


def create_connection(db_name, db_user, db_password, db_host, db_port):
    ''' Function to Create Connection to DB '''

    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

# Replace the following details with your PostgreSQL credentials
# Order of Params : dbname, user, password, host, port
connection = create_connection(
    "sectest", 
    "postgres",
    "pass",
    "localhost",
    "5432"
    )

cursor = connection.cursor()



def insert_into_db(files):
    ''' 
    Function reads each .txt file into pandas dataframe

    Then > Generates: 
        1, Query to Create Table,
        2, Command to Insert from the file into the table

    Then > Executes the generated query and command
    
    '''

    for file in files:

        df = pd.read_csv(f"./extracted/{file}", sep = '\t')
        cols = list(df.columns)
        
        # Query for Creating the table
        create_table_query = f"CREATE TABLE IF NOT EXISTS {file[:-4]} ("
        
        # Command for inserting into the table
        insert_table_command = f"COPY public.{file[:-4]} ("

        for col in cols[:-1]:
            # Decided to make all columns as VARCHAR -> or touble
            # But must be transformed to proper datatype later in SQL
            create_table_query += f"{col} VARCHAR,"
            insert_table_command += f"{col},"

        create_table_query += f"{cols[-1]} VARCHAR );"

        absPath = os.path.abspath(f"./extracted/{file}")
        insert_table_command += f"{cols[-1]}) FROM '{absPath}' DELIMITER '\t';"

        cursor.execute(create_table_query)
        connection.commit()
        print(f"Table {file[:-4]} Created")

        cursor.execute(insert_table_command)
        connection.commit()
        print(f"Table {file[:-4]} Inserted")



# Get list of files -> num.txt sub.txt pre.txt tag.txt
files = []
for file in os.listdir("./extracted/"):
    if file.endswith(".txt"):
        files.append(file)

# Call func to Insert the files into DB
insert_into_db(files)

# Deleting as extracted dir no longer needed
os.rmdir(absPath)


cursor.close()
connection.close()


