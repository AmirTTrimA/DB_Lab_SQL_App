import pyodbc

def list_drivers():
    drivers = pyodbc.drivers()
    print("Available ODBC drivers:")
    for driver in drivers:
        print(f"  - {driver}")

if __name__ == "__main__":
    list_drivers() 