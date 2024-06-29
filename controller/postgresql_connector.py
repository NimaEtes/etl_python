import psycopg2

class PostgreSQLConnector:
    def __init__(self, params):
        self.params = params
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.params)
            self.cursor = self.connection.cursor()
            print("Connected to PostgreSQL!")
        except psycopg2.Error as e:
            print(f"Error connecting to PostgreSQL: {e}")
            exit()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("PostgreSQL connection closed.")
