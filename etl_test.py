import psycopg2
import pyodbc

# PostgreSQL connection parameters
pg_conn_params = {
    "host": "127.0.0.1",
    "port": "5432",
    "database": "nestech",
    "user": "postgres",
    "password": "sq.B7ZTSXtnUKd;w"
}

# SQL Server connection parameters
sql_server_conn_str = (
    "DRIVER={SQL Server};"
    "SERVER=WIN-V4OGBGQUHNL;"
    "DATABASE=nestech_datawarehouse;"
    "UID=sa;"
    "PWD=S@123456"
)

# Connect to PostgreSQL
try:
    pg_conn = psycopg2.connect(**pg_conn_params)
    pg_cursor = pg_conn.cursor()
    print("Connected to PostgreSQL!")
except psycopg2.Error as e:
    print(f"Error connecting to PostgreSQL: {e}")
    exit()

# Connect to SQL Server
try:
    sql_server_conn = pyodbc.connect(sql_server_conn_str)
    sql_server_cursor = sql_server_conn.cursor()
    print("Connected to SQL Server!")
except pyodbc.Error as e:
    print(f"Error connecting to SQL Server: {e}")
    pg_cursor.close()
    pg_conn.close()
    exit()

# Check if tables exist in SQL Server and create if not
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
        sql_server_cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table}') {create_query}")
        sql_server_conn.commit()
        print(f"Checked and ensured creation of table: {table}")
    except pyodbc.Error as e:
        print(f"Error creating table {table}: {e}")
        pg_cursor.close()
        pg_conn.close()
        sql_server_cursor.close()
        sql_server_conn.close()
        exit()

# Extract data from PostgreSQL (Odoo HR module)
try:
    pg_query_dim_employee = """
        SELECT id AS employee_id, name, job_title, department_id, create_date AS hire_date
        FROM hr_employee;
    """
    pg_cursor.execute(pg_query_dim_employee)
    dim_employee_rows = pg_cursor.fetchall()

    pg_query_dim_department = """
        SELECT id AS department_id, name AS department_name, '' AS location
        FROM hr_department;
    """
    pg_cursor.execute(pg_query_dim_department)
    dim_department_rows = pg_cursor.fetchall()

    pg_query_fact_hr = """
        SELECT employee_id, department_id, EXTRACT(epoch FROM create_date)::BIGINT AS time_id, wage AS salary, 0 AS bonus
        FROM hr_contract;
    """
    pg_cursor.execute(pg_query_fact_hr)
    fact_hr_rows = pg_cursor.fetchall()
except psycopg2.Error as e:
    print(f"Error fetching data from PostgreSQL: {e}")
    pg_cursor.close()
    pg_conn.close()
    sql_server_cursor.close()
    sql_server_conn.close()
    exit()

# Transform and load data into SQL Server
try:
    # Insert or update into dim_employee
    for row in dim_employee_rows:
        sql_server_cursor.execute("""
            IF EXISTS (SELECT 1 FROM dim_employee WHERE employee_id = ?)
                UPDATE dim_employee SET name = ?, job_title = ?, department = ?, hire_date = ? WHERE employee_id = ?
            ELSE
                INSERT INTO dim_employee (employee_id, name, job_title, department, hire_date)
                VALUES (?, ?, ?, ?, ?);
        """, (row[0], row[1], row[2], row[3], row[4], row[0], row[0], row[1], row[2], row[3], row[4]))

    # Insert or update into dim_department
    for row in dim_department_rows:
        sql_server_cursor.execute("""
            IF EXISTS (SELECT 1 FROM dim_department WHERE department_id = ?)
                UPDATE dim_department SET department_name = ?, location = ? WHERE department_id = ?
            ELSE
                INSERT INTO dim_department (department_id, department_name, location)
                VALUES (?, ?, ?);
        """, (row[0], row[1], row[2], row[0], row[0], row[1], row[2]))

    # Insert or update into fact_hr
    for row in fact_hr_rows:
        sql_server_cursor.execute("""
            IF EXISTS (SELECT 1 FROM fact_hr WHERE employee_id = ? AND department_id = ? AND time_id = ?)
                UPDATE fact_hr SET salary = ?, bonus = ? WHERE employee_id = ? AND department_id = ? AND time_id = ?
            ELSE
                INSERT INTO fact_hr (employee_id, department_id, time_id, salary, bonus)
                VALUES (?, ?, ?, ?, ?);
        """, (row[0], row[1], row[2], row[3], row[4], row[0], row[1], row[2], row[0], row[1], row[2], row[3], row[4]))

    sql_server_conn.commit()
    print("Data loaded into SQL Server successfully!")
except pyodbc.Error as e:
    print(f"Error inserting data into SQL Server: {e}")
    pg_cursor.close()
    pg_conn.close()
    sql_server_cursor.close()
    sql_server_conn.close()
    exit()

# Close connections
pg_cursor.close()
pg_conn.close()
sql_server_cursor.close()
sql_server_conn.close()

print("Data transfer and table creation completed successfully!")