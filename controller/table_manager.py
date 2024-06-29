import pyodbc


class TableManager:
    def __init__(self, cursor, connection):
        self.cursor = cursor
        self.connection = connection

    def create_tables(self):
        tables_to_create = {
            "dim_employee": """
                CREATE TABLE dim_employee (
                    employee_id INT PRIMARY KEY,
                    name NVARCHAR(255),
                    job_title NVARCHAR(255),
                    department NVARCHAR(255),
                    hire_date DATE
                )
            """,
            "dim_department": """
                CREATE TABLE dim_department (
                    department_id INT PRIMARY KEY,
                    department_name NVARCHAR(255),
                    location NVARCHAR(255)
                )
            """,
            "dim_time": """
                CREATE TABLE dim_time (
                    time_id INT PRIMARY KEY,
                    date DATE,
                    year INT,
                    quarter INT,
                    month INT,
                    day INT,
                    week INT,
                    weekday NVARCHAR(10)
                )
            """,
            "fact_hr": """
                CREATE TABLE fact_hr (
                    fact_id INT PRIMARY KEY IDENTITY(1,1),
                    employee_id INT,
                    department_id INT,
                    time_id INT,
                    salary DECIMAL(18, 2),
                    bonus DECIMAL(18, 2),
                    FOREIGN KEY (employee_id) REFERENCES dim_employee(employee_id),
                    FOREIGN KEY (department_id) REFERENCES dim_department(department_id),
                    FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
                )
            """
        }

        for table, create_query in tables_to_create.items():
            try:
                # Check if table exists
                self.cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}'")
                if not self.cursor.fetchone():
                    self.cursor.execute(create_query)
                    self.connection.commit()
                    print(f"Created table: {table}")
                else:
                    print(f"Table {table} already exists.")
            except pyodbc.Error as e:
                print(f"Error creating table {table}: {e}")
                self.cursor.close()
                self.connection.close()
                exit()
