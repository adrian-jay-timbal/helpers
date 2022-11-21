# sqlite csv to database

# import libraries
import sqlite3 # DBMS
import pandas as pd # for data manipulation
from sqlite3 import Error # error manipulation
from tqdm import tqdm # progress bar


# function to check input error
def input_check(input, matches):
    if any(x in input for x in matches):
        print("Input is valid, proceeding now.")
        return
    else:
        print("Invalid input, program suspended.")
        print("Try using double \\ instead.")
        exit()
        
# function for database connection
def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
        
    return connection

# function to execute sql queries
def execute_query(query, values):
    try:
        cursor.execute(query, values)
            
    except Error as e:
        print(f"The error '{e}' occured")
        

# checking user input for database
database_path = input("SQLite database location: ")
db_indicator = [".db", ".sqlite", "sqlite3", ".db3", "\\"]
input_check(database_path, db_indicator)

# calling and defining connection to database
db_connection = create_connection(database_path)
cursor = db_connection.cursor()



def csv_to_dbase():
    # defining csv file to be processed
    # check csv file input
    file_name = input("Enter csv file: ")
    csv_indicator = [".csv"]
    input_check(file_name, csv_indicator)

    # using pandas library to read the data from csv to a dataframe
    file_data = pd.read_csv(file_name)
    p_len = len(file_data)
    data_count = 0


    # add and customize field names according to your data
    # itirating through the dataframe and inserting into database
    # tqdm is for the progress bar
    for index, row in tqdm(file_data.iterrows()):

        sql_query = "INSERT INTO bankchurnners (client_id, card_category, income_category, credit_limit, balance) VALUES (?, ?, ?, ?, ?)" # sql fields
        # CLIENTNUM, Card_Category and the rest are csv fieldnames
        sql_values = (row.CLIENTNUM, row.Card_Category, row.Income_Category, row.Credit_Limit, row.Total_Revolving_Bal)
        
        execute_query(sql_query, sql_values)
        data_count += 1
        
    db_connection.commit()
    print(f"Successful, {data_count} data transferred out of {p_len}")

def dbase_to_csv():
    
    sql_query = pd.read_sql_query(input("Enter you query:\n") ,db_connection)
    df = pd.DataFrame(sql_query, columns = ["client_id", "user_id", "card_category", "income_category", "credit_limit", "balance"])
    new_csv = input("Enter csv filename to be created\nDont forget the .csv extension: ")
    df.to_csv(new_csv)
    print("Done, Bye bye!")
    
    
def main():
    user_choice = int(input("1. csvTOsqlite\n2. sqliteTOcsv\nEnter the number of your choice: "))
    if user_choice == 1:
        csv_to_dbase()
    elif user_choice == 2:
        dbase_to_csv()
    else:
        print("You need to choose a number between the options.")
        main()

print("Good day!\nWhat do you want me to help you with?")    
main()

cursor.close()
db_connection.close()
# 11/21/2022