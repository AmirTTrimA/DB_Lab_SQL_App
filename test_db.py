import pyodbc
from dotenv import load_dotenv
import os

# Clear any existing env variables
if "DATABASE_URL" in os.environ:
    del os.environ["DATABASE_URL2"]

# Load the .env file
load_dotenv(override=True)

conn_str = os.getenv("DATABASE_URL2")
print(f"Connection string: {conn_str}")

try:
    conn = pyodbc.connect(conn_str)
    print("Connection successful!")
    cursor = conn.cursor()
    cursor.execute("SELECT @@VERSION")
    row = cursor.fetchone()
    print(f"SQL Server version: {row[0]}")
    conn.close()
except pyodbc.Error as e:
    print(f"Error connecting to database: {str(e)}")
