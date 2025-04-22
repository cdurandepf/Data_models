import mysql.connector
import os
from datetime import datetime
import time

user = 'root'
db_name = "flight_reservation_5"
password="we4reDATA"



def log_timestamp(step_name):
    start_time = time.time()  # Capture the start time
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {step_name} started")
    return start_time  # Return the start time to calculate the duration later

def log_elapsed_time(start_time, step_name):
    elapsed_time = time.time() - start_time  # Calculate the elapsed time
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {step_name} took {elapsed_time:.2f} seconds")

def check_duplicate_seats(user, password, db_name):
    start_time = log_timestamp("Starting the check for duplicate seats")

    try:
        start_time_conn = log_timestamp("Connecting to the database")
        connection = mysql.connector.connect(user=user, password=password,
                                             host='127.0.0.1', database=db_name,
                                             connection_timeout=60)
        cursor = connection.cursor()
        log_elapsed_time(start_time_conn, "Database connection established")

        query = """
        SELECT
            r1.passenger_id,
            r2.passenger_id, 
            r1.seat AS seat_number
        FROM 
            Reserve r1
        JOIN 
            Reserve r2 ON r1.seat = r2.seat AND r1.passenger_id != r2.passenger_id
        LIMIT 100
        """

        start_time_query = log_timestamp("Executing the query")
        cursor.execute(query)
        rows = cursor.fetchall()
        log_elapsed_time(start_time_query, "Query executed")

        if rows:
            log_timestamp("Duplicate seats detected")
            print("⚠️ Conflits de sièges détectés :")
            for row in rows:
                print(f"Vol {row[0]} : passager {row[1]} et {row[2]} ont le même siège ({row[1]})")
        else:
            log_timestamp("No duplicate seats found")
            print("✅ Aucun conflit de sièges détecté.")

    except mysql.connector.Error as err:
        log_timestamp("Error occurred while executing the query")
        print(f"Erreur lors de l'exécution de la requête : {err}")
    finally:
        if connection.is_connected():
            start_time_close = log_timestamp("Closing the connection")
            cursor.close()
            connection.close()
            log_elapsed_time(start_time_close, "Connection closed")

    log_elapsed_time(start_time, "Check for duplicate seats completed")

check_duplicate_seats(user, password, db_name)


def superposition_flights(user, password, db_name):
    
    try:
        connection = mysql.connector.connect(user=user, password=password,
                                             host='127.0.0.1', database=db_name)
        cursor = connection.cursor()

        query = """
        SELECT  # We want to select the passenger id and both of the flight that are overlapping
            r1.passenger_id, 
            f1.flight_id AS flight_1, 
            f2.flight_id AS flight_2
        FROM 
            Reserve r1 
        # We then join the table Booking, Flight and Flight_information to obtain the flight id, the start # of the flight and its duration
        JOIN 
            Booking b1 ON r1.booking_id = b1.booking_id 
        JOIN 
            Flight f1 ON b1.flight_id = f1.flight_id 
        JOIN 
            Flight_information fi1 ON f1.route = fi1.route 
        # We then find another flight for the same passenger (with the flight_id smaller than the previous to only check them once) 
        JOIN 
            Reserve r2 ON r1.passenger_id = r2.passenger_id 
        JOIN 
            Booking b2 ON r2.booking_id = b2.booking_id 
                    AND b1.flight_id < b2.flight_id 
        JOIN 
            Flight f2 ON b2.flight_id = f2.flight_id 
        JOIN 
            Flight_information fi2 ON f2.route = fi2.route 
        # We than check if the two flight are overlapping
        WHERE 
            f1.flight_day = f2.flight_day 
            AND (
                f1.flight_hour + fi1.flight_duration > f2.flight_hour 
                AND f1.flight_hour < f2.flight_hour + fi2.flight_duration
            );

        """

        cursor.execute(query)
        rows = cursor.fetchall()

        if rows:
            print(" Superposition detected ")
            for row in rows:
                print(f"Passenger {row[0]} : {row[1]} and {row[2]} are at the same time")
        else:
            print(" No superposition detected ")

    except mysql.connector.Error as err:
        print(f"Erreur lors de l'exécution de la requête : {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def round_trip(user, password, db_name):
    
    try:
        connection = mysql.connector.connect(user=user, password=password,
                                             host='127.0.0.1', database=db_name)
        cursor = connection.cursor()

        query = """

            SELECT R.passenger_id
            FROM Reserve R
            JOIN Booking B ON R.booking_id = B.booking_id
            WHERE B.trip_type = 'RoundTrip'
            GROUP BY R.passenger_id
            HAVING COUNT(B.flight_id) = 1;

        """

        cursor.execute(query)
        rows = cursor.fetchall()

        if rows:
            print(" No other flight ! ")
            
        else:
            print(" Other flight okay  ")

    except mysql.connector.Error as err:
        print(f"Erreur lors de l'exécution de la requête : {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
