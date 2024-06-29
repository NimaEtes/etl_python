from model.etl_controller import ETLController

# PostgreSQL connection parameters
pg_conn_params = {
    "host": "127.0.0.1",
    "port": "5432",
    "database": "nestech",
    "user": "postgres",
    "password": "sq.B7ZTSXtnUKd;w"
}

# SQL Server connection string
sql_conn_str = (
    "DRIVER={SQL Server};"
    "SERVER=WIN-V4OGBGQUHNL;"
    "DATABASE=nestech_datawarehouse;"
    "UID=sa;"
    "PWD=S@123456"
)

if __name__ == "__main__":
    etl_controller = ETLController(pg_conn_params, sql_conn_str)
    etl_controller.run()
