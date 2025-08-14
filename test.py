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


def write_to_file(rows, filename):
    # Write results to a CSV file
    try:
        filepath = os.path.join(LOCATION_FOLDER, filename + ".dat")
        with open(filename, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(" ".join(map(str, row)) + "\n") # print space between each column
        print(f"Results written to {filepath}")
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
        enrollment_rows, enrollment_headers = run_query(cursor, "SELECT * FROM eftps_enrollments_stage")
        if enrollment_rows:
            file_date = str(enrollment_rows[0][enrollment_headers.index("file_date")])
            file_seq_num = str(enrollment_rows[0][enrollment_headers.index("file_sequence_number")])
            


        # write_to_file(cursor, "output.csv")
        # close_connection(mydb, cursor)
        # write_to_file