import mysql
import mysql.connector

from config import config as cfg
from config import config_dev as cfgdev
from sshtunnel import SSHTunnelForwarder
from src.queries_execution import execute_query
from src.api_data_retrieve import load_data_to_database
from src.queries_db_script import query_1, query_2, query_3, query_4, query_5, query_6
from src.create_db_script import download_and_extract_dataset, create_database_schema, drop_all_tables


def main():
    """
    This function serves as the central hub of the script, managing multiple critical tasks:
    Downloading and extracting the dataset.
    Establishing a secure SSH tunnel for port forwarding.
    Connecting to a MySQL database.
    Updating the database schema.
    Populating the database.
    Running various SQL queries.
    """
    download_and_extract_dataset()

    print("establishing SSH tunnel...")
    with SSHTunnelForwarder(
            ssh_address_or_host=cfgdev.SSH_CONFIG['ssh_address_or_host'],
            ssh_username=cfgdev.SSH_CONFIG['ssh_username'],
            # ssh_password=cfgdev.SSH_CONFIG['ssh_password'],
            ssh_pkey=cfgdev.SSH_CONFIG['ssh_pkey'],  # Use OpenSSH key
            remote_bind_address=cfgdev.SSH_CONFIG['remote_bind_address'],
            local_bind_address=cfgdev.SSH_CONFIG['local_bind_address']
    ) as tunnel:

        print(f"tunnel established: Local port {tunnel.local_bind_port}")

        try:
            print("connecting to MySQL server...")
            connection = mysql.connector.connect(
                host=cfg.DB_CONFIG['host'],
                port=tunnel.local_bind_port,
                user=cfg.DB_CONFIG['user'],
                password=cfg.DB_CONFIG['password'],
                database=cfg.DB_CONFIG['database'],
                connection_timeout=60
            )

            cursor = connection.cursor(prepared=True)   # Create a MySQLCursorPrepared cursor to enable prepared statements

            # inp = input("drop all tables? (yes)")
            # if inp.lower() == "yes":
            #     drop_all_tables(cursor, connection)

            # create_database_schema(cursor)
            # load_data_to_database(cursor, connection)

            try:
                execute_query(connection, query_1, "future galaxy")
                execute_query(connection, query_2, "Skarsgard")
                execute_query(connection, query_3, "Comedy")
                execute_query(connection, query_4, "Drama")
                execute_query(connection, query_5)
                execute_query(connection, query_6, "The Hitchhiker's Guide to the Galaxy")
            except mysql.connector.Error as error:
                print("Failed to execute query: {}".format(error))

        except mysql.connector.Error as err:
            print(f"MySQL connection error: {err}")

        finally:
            if 'connection' in locals() and connection.is_connected():
                connection.close()
                print("MySQL connection closed")


if __name__ == '__main__':
    main()
