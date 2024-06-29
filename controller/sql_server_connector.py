import pyodbc

class SQLServerConnector:
    def __init__(self, conn_str):
        self.conn_str = conn_str
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = pyodbc.connect(self.conn_str)
            self.cursor = self.connection.cursor()
            print("Connected to SQL Server!")
        except pyodbc.Error as e:
            print(f"Error connecting to SQL Server: {e}")
            exit()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("SQL Server connection closed.")
