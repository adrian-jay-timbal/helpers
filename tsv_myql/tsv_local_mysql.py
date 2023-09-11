import os
import re
import mysql.connector

# Database configuration
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "admin",
    "database": "airportdb",
    "allow_local_infile": True,
}

# Define the directory where the TSV files are located
tsv_directory = "tsv_folder"

# Initialize a MySQL connection
connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

# Function to extract table name from filename (excluding numbers)
def extract_table_name(filename):
    table_name = os.path.splitext(filename)[0]  # Remove file extension
    table_name = re.sub(r'\d', '', table_name)  # Remove numbers
    return table_name

# Iterate through CSV files in the directory
for root, _, files in os.walk(tsv_directory):
    for file in files:
        if file.endswith(".tsv"):

            # Remove '@', '0' and 'airportdb' from the file name
            new_file_name = os.path.basename(file).replace('@', '').replace('0', '').replace('airportdb', '')
            
            # Extract the table name from the file name
            table_name = extract_table_name(new_file_name)

            # Construct the full path to the TSV file
            tsv_file_path = os.path.join(root, file)

            # Replace backslashes with forward slashes for Windows paths
            tsv_file_path = tsv_file_path.replace("\\", "/")

            # Define the SQL query to load data from the TSV file
            load_data_query = f"""
                LOAD DATA LOCAL INFILE '{tsv_file_path}' 
                INTO TABLE {table_name} 
                FIELDS TERMINATED BY '\t' 
                LINES TERMINATED BY '\n';
            """

            # Execute the SQL query to load data
            try:
                cursor.execute(load_data_query)
                print(f"Loaded data from '{tsv_file_path}' into table '{table_name}'")
            except mysql.connector.Error as err:
                print(f"Error loading data from '{tsv_file_path}' into table '{table_name}': {err}")

# Commit changes and close the connection
connection.commit()
connection.close()

print("Data loading completed.")
