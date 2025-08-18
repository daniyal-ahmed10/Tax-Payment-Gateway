import mysql.connector
import os

# Configuration
LOCATION_FOLDER = r"C:\Temp\EFTPS"

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "password",
    "database": "world"
}


# Functions
def make_connection():
    # Establish MySQL connection and cursor 
    try:
        mydb = mysql.connector.connect(**DB_CONFIG)
        print("Connected to MySQL database!")
        return mydb, mydb.cursor()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None, None


def run_query(cursor, query):
    # Execute SQL query using the cursor
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        headers = [col[0] for col in cursor.description]
        return rows, headers
    except mysql.connector.Error as err:
        print(f"Error executing query: {err}")
        return [], []


# def write_to_file(rows, filename):
#     # Write results to a CSV file
#     try:
#         filepath = os.path.join(LOCATION_FOLDER, filename + ".dat")
#         with open(filepath, "w", encoding="utf-8") as f:
#             for row in rows:
#                 f.write(" ".join(map(str, row)) + "\n") # print space between each column
#         print(f"Results written to {filepath}")
#     except Exception as e:
#         print(f"Error writing to file: {e}")

def write_to_file(rows, filename):
    # Write results to a .dat file
    if not rows:
        return
    
    filepath = os.path.join(LOCATION_FOLDER, filename + ".dat")
    
    # find max width for each column
    col_widths = [max(len(str(row[i])) for row in rows) for i in range(len(rows[0]))]
    
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            for row in rows:
                line = "  ".join(str(val).ljust(col_widths[i]) for i, val in enumerate(row))
                f.write(line + "\n")
        print(f"File written: {filepath}")
    except Exception as e:
        print(f"Error writing to file: {e}")


def close_connection(mydb, cursor):
    # Close connection 
    if cursor != None:
        cursor.close()
    if mydb != None:
        mydb.close()
    print("MySQL connection closed.")


# Main program
if __name__ == "__main__":
    mydb, cursor = make_connection()
    if mydb and cursor != None:
        # Extract enrollments data
        enrollment_rows, enrollment_headers = run_query(cursor, "SELECT * FROM eftps_enrollments_stage")
        if enrollment_rows:
            # Generate file name
            file_date = str(enrollment_rows[0][enrollment_headers.index("file_date")])
            file_seq_num = str(enrollment_rows[0][enrollment_headers.index("file_sequence_number")])
            enrollment_name = f"eftps_enrollments_{file_date}_{file_seq_num}"
            write_to_file(enrollment_rows, enrollment_name)

        # Extract payments data
        payment_rows, payment_headers = run_query(cursor, "SELECT * FROM eftps_payments_stage")
        if payment_rows:
            # Generate file name
            file_date = str(payment_rows[0][payment_headers.index("file_date")])
            file_seq_num = str(payment_rows[0][payment_headers.index("file_sequence_number")])
            payment_name = f"eftps_payments_{file_date}_{file_seq_num}"
            write_to_file(payment_rows, payment_name)

        close_connection(mydb, cursor)