import psycopg2
import pyodbc


class DataModel:
    def __init__(self, pg_cursor, sql_cursor, sql_connection):
        self.pg_cursor = pg_cursor
        self.sql_cursor = sql_cursor
        self.sql_connection = sql_connection

    def extract_data(self):
        try:
            pg_query_dim_employee = """
                SELECT id AS employee_id, name, job_title, department_id, create_date AS hire_date
                FROM hr_employee;
            """
            self.pg_cursor.execute(pg_query_dim_employee)
            dim_employee_rows = self.pg_cursor.fetchall()

            pg_query_dim_department = """
                SELECT id AS department_id, name AS department_name, '' AS location
                FROM hr_department;
            """
            self.pg_cursor.execute(pg_query_dim_department)
            dim_department_rows = self.pg_cursor.fetchall()

            pg_query_fact_hr = """
                SELECT employee_id, department_id, EXTRACT(epoch FROM create_date)::BIGINT AS time_id, wage AS salary, 0 AS bonus
                FROM hr_contract;
            """
            self.pg_cursor.execute(pg_query_fact_hr)
            fact_hr_rows = self.pg_cursor.fetchall()

            return dim_employee_rows, dim_department_rows, fact_hr_rows
        except psycopg2.Error as e:
            print(f"Error fetching data from PostgreSQL: {e}")
            self.pg_cursor.close()
            self.pg_conn.close()
            self.sql_cursor.close()
            self.sql_connection.close()
            exit()

    def load_data(self, dim_employee_rows, dim_department_rows, fact_hr_rows):
        try:
            # Insert or update into dim_employee
            for row in dim_employee_rows:
                self.sql_cursor.execute("""
                    IF EXISTS (SELECT 1 FROM dim_employee WHERE employee_id = ?)
                        UPDATE dim_employee SET name = ?, job_title = ?, department = ?, hire_date = ? WHERE employee_id = ?
                    ELSE
                        INSERT INTO dim_employee (employee_id, name, job_title, department, hire_date)
                        VALUES (?, ?, ?, ?, ?);
                """, (row[0], row[1], row[2], row[3], row[4], row[0], row[0], row[1], row[2], row[3], row[4]))

            # Insert or update into dim_department
            for row in dim_department_rows:
                self.sql_cursor.execute("""
                    IF EXISTS (SELECT 1 FROM dim_department WHERE department_id = ?)
                        UPDATE dim_department SET department_name = ?, location = ? WHERE department_id = ?
                    ELSE
                        INSERT INTO dim_department (department_id, department_name, location)
                        VALUES (?, ?, ?);
                """, (row[0], row[1], row[2], row[0], row[0], row[1], row[2]))

            # Insert or update into fact_hr
            for row in fact_hr_rows:
                self.sql_cursor.execute("""
                    IF EXISTS (SELECT 1 FROM fact_hr WHERE employee_id = ? AND department_id = ? AND time_id = ?)
                        UPDATE fact_hr SET salary = ?, bonus = ? WHERE employee_id = ? AND department_id = ? AND time_id = ?
                    ELSE
                        INSERT INTO fact_hr (employee_id, department_id, time_id, salary, bonus)
                        VALUES (?, ?, ?, ?, ?);
                """, (row[0], row[1], row[2], row[3], row[4], row[0], row[1], row[2], row[0], row[1], row[2], row[3], row[4]))

            self.sql_connection.commit()
            print("Data loaded into SQL Server successfully!")
        except pyodbc.Error as e:
            print(f"Error inserting data into SQL Server: {e}")
            self.pg_cursor.close()
            self.pg_conn.close()
            self.sql_cursor.close()
            self.sql_connection.close()
            exit()
