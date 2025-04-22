import os
import mysql.connector
from threading import Thread
from queue import Queue
from time import time
from datetime import datetime

# Helper functions
def log_timestamp(step_name):
    start = time()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {step_name} started")
    return start

def log_elapsed_time(start, step_name):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {step_name} took {time() - start:.2f} seconds")

# Function to execute SQL commands in a thread
def execute_chunk(sql_chunk, user, password, db_name, thread_id):
    try:
        connection = mysql.connector.connect(
            user=user,
            password=password,
            host='127.0.0.1',
            database=db_name
        )
        cursor = connection.cursor()
        for command in sql_chunk:
            if command.strip():
                try:
                    cursor.execute(command)
                except mysql.connector.Error as err:
                    print(f"[Thread {thread_id}] Error executing command: {command[:50]}...\nError: {err}")
        connection.commit()
    except mysql.connector.Error as err:
        print(f"[Thread {thread_id}] Connection error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Main function to import with multithreading
def import_sql_dump_multithreaded(dump_file, user, password, db_name, num_threads=4):
    start_time = log_timestamp("Importing SQL dump (multi-threaded)")

    if not os.path.exists(dump_file):
        print(f"Error: Dump file '{dump_file}' not found.")
        return

    # Load and split SQL file
    with open(dump_file, 'r', encoding='utf-8') as file:
        commands = [cmd.strip() for cmd in file.read().split(';') if cmd.strip()]

    # Split into chunks for threads
    chunk_size = len(commands) // num_threads + 1
    chunks = [commands[i:i + chunk_size] for i in range(0, len(commands), chunk_size)]

    # Launch threads
    threads = []
    for idx, chunk in enumerate(chunks):
        thread = Thread(target=execute_chunk, args=(chunk, user, password, db_name, idx + 1))
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    log_elapsed_time(start_time, "SQL dump import (multi-threaded)")
