import mysql
import mysql.connector
from sshtunnel import SSHTunnelForwarder
from config import config as cfg
from src.api_data_retrieve import load_data_to_database
from src.create_db_script import download_and_extract_dataset, create_database_schema, drop_all_tables


def main():

    download_and_extract_dataset()

    print("establishing SSH tunnel...")
    with SSHTunnelForwarder(
            ssh_address_or_host=cfg.SSH_CONFIG['ssh_address_or_host'],
            ssh_username=cfg.SSH_CONFIG['ssh_username'],
            ssh_password=cfg.SSH_CONFIG['ssh_password'],
            remote_bind_address=cfg.SSH_CONFIG['remote_bind_address'],
            local_bind_address=cfg.SSH_CONFIG['local_bind_address']
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

            cursor = connection.cursor()

            inp = input("drop all tables? (y/n)")
            if inp.lower() == "y":
                drop_all_tables(cursor, connection)

            create_database_schema(cursor)
            load_data_to_database(cursor, connection)

        except mysql.connector.Error as err:
            print(f"MySQL connection error: {err}")

        finally:
            if 'connection' in locals() and connection.is_connected():
                connection.close()
                print("MySQL connection closed")

if __name__ == '__main__':
    main()
