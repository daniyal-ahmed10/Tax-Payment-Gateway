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
        log_errors(
            table="enrollment_errors",  # fallback table
            error_code="CONN",
            error_message=str(err)
        )
        return None, None


def run_query(cursor, query, table_name=None, file_date=None, file_sequence_number=None):
    # Execute SQL query using the cursor
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        headers = [col[0] for col in cursor.description]
        return rows, headers
    except mysql.connector.Error as err:
        print(f"Error executing query: {err}") # testing
        if table_name:
            log_errors(
                table=table_name,
                error_code="QRY",
                error_message=str(err),
                file_date=file_date,
                file_sequence_number=file_sequence_number
            )  
        return [], []


def write_to_file(rows, filename, headers, file_date=None, file_sequence_number=None):
    # Write results to a .dat file
    if not rows:
        return
    
    filepath = os.path.join(LOCATION_FOLDER, filename + ".dat")

    # Determine table type
    table_name = "enrollment_errors" if "taxpayer_pin" in headers else "payments_errors"

    
    # find max width for each column
    col_widths = [max(len(str(row[i])) for row in rows) for i in range(len(rows[0]))]
    
    try:
        for row in rows:
            # Example validation: missing TIN
            tin_index = headers.index("taxpayer_tin") if "taxpayer_tin" in headers else None
            if tin_index is not None and not row[tin_index]:
                log_errors(
                    table=table_name,
                    error_code="DATA",
                    error_message="Missing taxpayer TIN",
                    file_date=file_date,
                    file_sequence_number=file_sequence_number,
                    taxpayer_tin=None,
                    taxpayer_pin=row[headers.index("taxpayer_pin")] if "taxpayer_pin" in headers else None
                )
        with open(filepath, "w", encoding="utf-8") as f:
            for row in rows:
                line = "  ".join(str(val).ljust(col_widths[i]) for i, val in enumerate(row))
                f.write(line + "\n")
        print(f"File written: {filepath}")
    except Exception as e:
        # print(f"Error writing to file: {e}")
        if table_name:
            log_errors(
                table=table_name,
                error_code="FILE",
                error_message=str(e),
                file_date=file_date,
                file_sequence_number=file_sequence_number
            )


# write log errors function
def log_errors(table, error_code, error_message, **kwargs):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    if table == "enrollment_errors":
        query = """
            INSERT INTO enrollment_errors
            (file_date, file_sequence_number, taxpayer_tin, taxpayer_pin, error_code, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (
            kwargs.get("file_date"),
            kwargs.get("file_sequence_number"),
            kwargs.get("taxpayer_tin"),
            kwargs.get("taxpayer_pin"),
            error_code,
            error_message
        )
    elif table == "payments_errors":
        query = """
            INSERT INTO payments_errors
            (file_date, file_sequence_number, taxpayer_tin, taxpayer_pin, error_code, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            kwargs.get("file_date"),
            kwargs.get("file_sequence_number"),
            kwargs.get("taxpayer_tin"),
            kwargs.get("taxpayer_pin"),
            error_code,
            error_message
        )
    else:
        raise ValueError("Invalid table name")

    cursor.execute(query, values)
    conn.commit()
    cursor.close()
    conn.close()

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
        enrollment_rows, enrollment_headers = run_query(cursor, "SELECT * FROM eftps_enrollments_stag", table_name = "enrollment_errors")
        if enrollment_rows:
            # Generate file name
            file_date = str(enrollment_rows[0][enrollment_headers.index("file_date")])
            file_seq_num = str(enrollment_rows[0][enrollment_headers.index("file_sequence_number")])
            enrollment_name = f"eftps_enrollments_{file_date}_{file_seq_num}"
            write_to_file(
                enrollment_rows,
                enrollment_name,
                enrollment_headers,
                file_date=file_date,
                file_sequence_number=file_seq_num
            )
        # Extract payments data
        payment_rows, payment_headers = run_query(cursor, "SELECT * FROM eftps_payments_stage")
        if payment_rows:
            # Generate file name
            file_date = str(payment_rows[0][payment_headers.index("file_date")])
            file_seq_num = str(payment_rows[0][payment_headers.index("file_sequence_number")])
            payment_name = f"eftps_payments_{file_date}_{file_seq_num}"
            write_to_file(
                payment_rows,
                payment_name,
                payment_headers,
                file_date=file_date,
                file_sequence_number=file_seq_num
            )

        close_connection(mydb, cursor)