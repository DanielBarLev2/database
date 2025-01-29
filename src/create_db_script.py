"""Contains code responsible for creating the database"""

import os
import pandas as pd
import mysql.connector
from sshtunnel import SSHTunnelForwarder

from config import config as cfg
from utils.load_data_utils import get_table_columns, insert_data, process_json_column, insert_foreign_data


def download_and_extract_dataset():
    """
    Creates a directory named /data and downloads and unzip the dataset from .kaggle.

    Expected files:
        data/tmdb_5000_credits.csv
        data/tmdb_5000_movies.csv
    """
    # set the path to the directory containing .kaggle.json
    kaggle_config_path = os.path.join(os.getcwd(), ".kaggle")
    os.environ["KAGGLE_CONFIG_DIR"] = kaggle_config_path

    try:
        os.makedirs(cfg.DOWNLOAD_PATH, exist_ok=False)
        try:
            # this import must come after changing environment path config
            import kaggle
            kaggle.api.authenticate()
            kaggle.api.dataset_download_files(cfg.DATASET_NAME, path=cfg.DOWNLOAD_PATH, unzip=True)
        except:
            raise ".kaggle.json is missing or invalid"

        print(f"dataset downloaded and unzipped and saved to {cfg.DOWNLOAD_PATH}.")

    except OSError:
        print(f"dataset is available at {cfg.DOWNLOAD_PATH}")


def create_database_schema(cursor):
    """
    Creates the database schema by executing SQL queries to create tables.

    :param cursor: A database cursor object used to execute SQL queries.
    :return: None
    """
    print("creating database schema...")

    tables = {
        "Movies":
        """ CREATE TABLE IF NOT EXISTS Movies (
                movie_id INT PRIMARY KEY,
                budget BIGINT,
                original_language VARCHAR(10),
                original_title VARCHAR(255),
                overview VARCHAR(1023),
                popularity FLOAT,
                release_date DATE,
                revenue BIGINT,
                runtime INT,
                status VARCHAR(15),
                title VARCHAR(255),
                vote_average FLOAT,
                vote_count INT
                );""",
        "Genres": """
            CREATE TABLE IF NOT EXISTS Genres (
                genre_id INT PRIMARY KEY,
                genre_name VARCHAR(100)
                );""",
        "Movies_Genres": """
            CREATE TABLE IF NOT EXISTS Movies_Genres (
                movie_id INT,
                genre_id INT,
                PRIMARY KEY (movie_id, genre_id),
                FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
                FOREIGN KEY (genre_id) REFERENCES Genres(genre_id)
                );""",

        "Keywords": """
            CREATE TABLE IF NOT EXISTS Keywords (
                keyword_id INT PRIMARY KEY,
                keyword_name VARCHAR(100)
                )""",

        "Movies_Keywords": """
                CREATE TABLE IF NOT EXISTS Movies_Keywords (
                    movie_id INT,
                    Keyword_id INT,
                    PRIMARY KEY (movie_id, Keyword_id),
                    FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
                    FOREIGN KEY (keyword_id) REFERENCES Keywords(keyword_id)
                    );"""
        }

    for table, query in tables.items():
        cursor.execute(query)
        print(f"+ {table} table was created")

    print("database schema created successfully.")


def drop_all_tables(cursor, connection):
    """
        Drops all tables in the current database.
    """
    print("deleting database schema...")
    try:
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()

        if not tables:
            print("No tables found in the database.")
            return

        # disable foreign key checks to avoid errors while dropping tables
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

        for (table_name,) in tables:
            cursor.execute(f"DROP TABLE {table_name};")
            print(f"- {table_name} table was dropped.")

        # re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

        connection.commit()
        print("database schema was dropped successfully.")

    except Exception as e:
        print(f"Error while dropping tables: {e}")
        connection.rollback()


def load_data_to_database(cursor, connection):
    """
        Load the dataset into the database.
        This is only done if the tables are empty, since its initialize the database.
    """
    print("loading database schema... (this might take a while :|)")

    # load datasets
    movies_data = pd.read_csv(cfg.MOVIE_DATA_PATH)
    _ = pd.read_csv(cfg.CREDITS_DATA_PATH)

    movies_df = movies_data.copy()
    movies_df.rename(columns={'id': 'movie_id'}, inplace=True) # dataset and sql-table first column name mismatch.
    movies_df = movies_df[get_table_columns(cursor=cursor, table_name="Movies")]
    insert_data(cursor=cursor, table_name="Movies", df=movies_df)

    genre_df = process_json_column(df=movies_data, column_name="genres")
    genre_df.columns = get_table_columns(cursor=cursor, table_name="Genres")
    genre_df = genre_df.drop_duplicates(subset=['genre_name'])
    insert_data(cursor=cursor, table_name="Genres", df=genre_df)

    insert_foreign_data(cursor=cursor, df=movies_data, column1='id', column2='genres', table_name="Movies_Genres")

    keyword_df = process_json_column(df=movies_data, column_name="keywords")
    keyword_df.columns = get_table_columns(cursor=cursor, table_name="Keywords")
    keyword_df = keyword_df.drop_duplicates(subset=['keyword_name'])
    insert_data(cursor=cursor, table_name="Keywords", df=keyword_df)

    insert_foreign_data(cursor=cursor, df=movies_data, column1='id', column2='keywords', table_name="Movies_Keywords")

    connection.commit()
    print("All data loading completed!")


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

            drop_all_tables(cursor, connection)
            create_database_schema(cursor)
            load_data_to_database(cursor, connection)


        except mysql.connector.Error as err:
            print(f"MySQL connection error: {err}")

        finally:
            if 'connection' in locals() and connection.is_connected():
                connection.close()
                print("MySQL connection closed")