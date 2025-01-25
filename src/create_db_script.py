"""Contains code responsible for creating the database"""

import os
import kaggle
import mysql.connector
from sshtunnel import SSHTunnelForwarder

from config import config as cfg


def download_and_extract_dataset():
    """
    Creates a directory named data and downloads and unzip the dataset from kaggle.

    Expected files:
        data/tmdb_5000_credits.csv
        data/tmdb_5000_movies.csv
    """
    try:
        os.makedirs(cfg.DOWNLOAD_PATH, exist_ok=False)

        try:
            kaggle.api.authenticate()
            kaggle.api.dataset_download_files(cfg.DATASET_NAME, path=cfg.DOWNLOAD_PATH, unzip=True)
        except:
            raise "kaggle.json must be placed in ~/.kaggle (usually at users/user)"

        print(f"dataset downloaded and unzipped and saved to {cfg.DOWNLOAD_PATH}.")

    except OSError:
        print(f"dataset is available at {cfg.DOWNLOAD_PATH}")


def create_database_schema(cursor):
    """
        @todo: Create the database schema.
    """
    print("Creating database schema...")


def load_data_to_database(cursor, connection):
    """
        @todo: Load the dataset into the database.
    """



def main():

    download_and_extract_dataset()

    print("Establishing SSH tunnel...")
    with SSHTunnelForwarder(ssh_address_or_host=cfg.SSH_CONFIG['ssh_address_or_host'],
                            ssh_username=cfg.SSH_CONFIG['ssh_username'],
                            ssh_password=None,
                            remote_bind_address=cfg.SSH_CONFIG['remote_bind_address'],
                            local_bind_address=cfg.SSH_CONFIG['local_bind_address']) as tunnel:

        print(f"Tunnel established: Local port {tunnel.local_bind_port}")

        try:
            print("Connecting to MySQL server...")
            connection = mysql.connector.connect(
                host=cfg.DB_CONFIG['host'],
                port=tunnel.local_bind_port,
                user=cfg.DB_CONFIG['user'],
                password=cfg.DB_CONFIG['password'],
                database=cfg.DB_CONFIG['database'],
                connection_timeout=60)

            print("Connected to MySQL server")

            cursor = connection.cursor()
            create_database_schema(cursor)
            print()
            load_data_to_database(cursor, connection)

        except mysql.connector.Error as err:
            print(f"MySQL connection error: {err}")

        finally:
            # finally close the connection
            if 'connection' in locals() and connection.is_connected():
                connection.close()
                print("MySQL connection closed")