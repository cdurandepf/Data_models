import os
import mysql.connector
from datetime import datetime
from time import time
from Ex_9 import check_duplicate_seats

user = 'root'
db_name = "flight_reservation_5"
password = "we4reDATA"
dump_file_path = 'benchmarking.sql'  # Replace with the actual PATH to your dump file

# Helper to log timestamps with messages
def log_timestamp(step_name):
    start_time = time()  # Capture the start time
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {step_name} started")
    return start_time  # Return the start time to calculate the duration later

def log_elapsed_time(start_time, step_name):
    elapsed_time = time() - start_time  # Calculate the elapsed time
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {step_name} took {elapsed_time:.2f} seconds")

# Step 1: Check if the database exists, and create it if it doesn't
start_time_db_check = log_timestamp("Checking/creating database")
try:
    connection = mysql.connector.connect(user=user, password=password, host='127.0.0.1')
    cursor = connection.cursor()
    cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
    result = cursor.fetchone()
    if not result:
        cursor.execute(f"CREATE DATABASE {db_name}")
        print(f"Database '{db_name}' created successfully.")
    else:
        print(f"Database '{db_name}' already exists.")
    cursor.close()
    connection.close()
    log_elapsed_time(start_time_db_check, "Database check/creation completed")
except mysql.connector.Error as err:
    print(f"Error: {err}")

# Step 2: Import the SQL dump
def import_sql_dump(dump_file, user, password, db_name):
    start_time_import = log_timestamp("Importing SQL dump")
    try:
        # Connect to the database
        connection = mysql.connector.connect(
            user=user,
            password=password,
            host='127.0.0.1',
            database=db_name
        )
        cursor = connection.cursor()

        # Check if the dump file exists
        if not os.path.exists(dump_file):
            print(f"Error: Dump file '{dump_file}' not found.")
            return

        # Read the SQL dump file
        with open(dump_file, 'r', encoding='utf-8') as file:
            sql_commands = file.read()

        # Execute the SQL commands
        for command in sql_commands.split(';'):
            command = command.strip()
            if command:
                try:
                    cursor.execute(command)
                except mysql.connector.Error as err:
                    print(f"Error executing command: {command}\nError: {err}")

        # Commit the changes
        connection.commit()
        print("SQL dump imported successfully.")
        log_elapsed_time(start_time_import, "SQL dump import completed")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

import_sql_dump(dump_file_path, user, password, db_name)

# Step 3: Run benchmark test - check for duplicate seats
start_time_benchmark = log_timestamp("Running check_duplicate_seats benchmark")
check_duplicate_seats(user, password, db_name)
log_elapsed_time(start_time_benchmark, "Benchmark completed")
