from controller.data_model import DataModel
from controller.postgresql_connector import PostgreSQLConnector
from controller.sql_server_connector import SQLServerConnector
from controller.table_manager import TableManager
from view.view import View


class ETLController:
    def __init__(self, pg_params, sql_conn_str):
        self.pg_params = pg_params
        self.sql_conn_str = sql_conn_str

    def run(self):
        # Connect to PostgreSQL
        pg_connector = PostgreSQLConnector(self.pg_params)
        pg_connector.connect()

        # Connect to SQL Server
        sql_server_connector = SQLServerConnector(self.sql_conn_str)
        sql_server_connector.connect()

        # Manage SQL Server tables
        table_manager = TableManager(sql_server_connector.cursor, sql_server_connector.connection)
        table_manager.create_tables()

        # Data Transfer
        data_model = DataModel(pg_connector.cursor, sql_server_connector.cursor, sql_server_connector.connection)
        dim_employee_rows, dim_department_rows, fact_hr_rows = data_model.extract_data()
        data_model.load_data(dim_employee_rows, dim_department_rows, fact_hr_rows)

        # Close connections
        pg_connector.close()
        sql_server_connector.close()

        # Display success message
        View.display_message("ETL process completed successfully!")
