"""Contains code responsible for creating the database"""

import os
import json

import pandas as pd
import mysql.connector
from sshtunnel import SSHTunnelForwarder

from config import config as cfg


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

    create_table_queries = {
        "create_movies_table": """
        CREATE TABLE IF NOT EXISTS Movies (
            movie_id INT PRIMARY KEY,
            title VARCHAR(255),
            budget BIGINT,
            original_language VARCHAR(10),
            original_title VARCHAR(255),
            release_date DATE,
            revenue BIGINT,
            runtime INT,
            status VARCHAR(50),
            vote_average FLOAT,
            vote_count INT,
            popularity FLOAT
            );
        """,
        "create_genres_table": """
        CREATE TABLE IF NOT EXISTS Genres (
            genre_id INT PRIMARY KEY,
            genre_name VARCHAR(100)
            );
        """,
        "create_movie_genres_table": """
        CREATE TABLE IF NOT EXISTS Movie_Genres (
            movie_id INT,
            genre_id INT,
            PRIMARY KEY (movie_id, genre_id),
            FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
            FOREIGN KEY (genre_id) REFERENCES Genres(genre_id)
            );
        """,
        "create_production_companies_table": """
        CREATE TABLE IF NOT EXISTS Production_Companies (
            company_id INT PRIMARY KEY,
            company_name VARCHAR(255)
            );
        """,
        "create_movie_companies_table": """
        CREATE TABLE IF NOT EXISTS Movie_Companies (
            movie_id INT,
            company_id INT,
            PRIMARY KEY (movie_id, company_id),
            FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
            FOREIGN KEY (company_id) REFERENCES Production_Companies(company_id)
            );
        """,
        "create_credits_table": """
        CREATE TABLE IF NOT EXISTS Credits (
            credit_id INT PRIMARY KEY,
            movie_id INT,
            person_id INT,
            role ENUM('actor', 'crew'),
            `character` VARCHAR(255),
            job VARCHAR(100),
            FOREIGN KEY (movie_id) REFERENCES Movies(movie_id)
            );
        """,
        "create_keywords_table": """
        CREATE TABLE IF NOT EXISTS Keywords (
            keyword_id INT PRIMARY KEY,
            keyword_name VARCHAR(255)
            );
        """,
        "create_movie_keywords_table": """
        CREATE TABLE IF NOT EXISTS Movie_Keywords (
            movie_id INT,
            keyword_id INT,
            PRIMARY KEY (movie_id, keyword_id),
            FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
            FOREIGN KEY (keyword_id) REFERENCES Keywords(keyword_id)
            );
        """}

    for table, query in create_table_queries.items():
        cursor.execute(query)

        table_name = table.split('_')[1]
        print(f"* {table_name} table was created")

    print("database schema created successfully!")


def drop_all_tables(cursor, connection):
    """
        Drops all tables in the current database.
    """
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
            print(f"* {table_name} table was dropped.")

        # re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

        connection.commit()
        print("All tables dropped successfully.")

    except Exception as e:
        print(f"Error while dropping tables: {e}")
        connection.rollback()


def load_data_to_database(cursor, connection):
    """
        Load the dataset into the database.
        This is only done if the tables are empty, since its initialize the database.
    """

    # load datasets
    movies_data = pd.read_csv(cfg.MOVIE_DATA_PATH)
    credits_data = pd.read_csv(cfg.CREDITS_DATA_PATH)

    def get_table_columns(_cursor, _table_name):
        """
        @:return: the column names of a given table.
        """
        _cursor.execute(f"DESCRIBE {_table_name}")
        return [_row[0] for _row in _cursor.fetchall()]

    def handle_missing_values(_data):
        """
            Handles missing values in the dataset by replacing them with defaults.
            - NaN Numeric columns are replaced with 0
            - NaN String columns are replaced with an empty string
            - Replace invalid or empty dates with NaT (Not a Time)
        """
        for column in _data.columns:
            if column == 'release_date':

                _data[column] = pd.to_datetime(_data[column], errors='coerce')
                _data[column] = _data[column].where(_data[column].notnull(), None)

            elif _data[column].dtype == 'object':
                _data[column].fillna('', inplace=True)

            else:
                _data[column].fillna(0, inplace=True)

        return _data

    def insert_data(_cursor, _table_name, _data):
        """
        Inserts data into a specified table.
        """
        # skips if already loaded
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = cursor.fetchone()[0]
        if row_count > 0:
            print(f"{table_name} table was already loaded. skip insertion")
            return

        _columns = get_table_columns(_cursor, _table_name)

        # change name from csv column -> sql table column
        column_mapping = {col: col for col in _columns}
        if "id" in _data.columns and "movie_id" in _columns:
            column_mapping["id"] = "movie_id"

        _data = _data.rename(columns=column_mapping)
        _data = _data[_columns]

        # handle missing values
        _data = handle_missing_values(_data)

        # Generate placeholders and query
        _placeholders = ", ".join(["%s"] * len(_columns))
        insert_query = f"""
            INSERT INTO {_table_name} ({', '.join(_columns)}) 
            VALUES ({_placeholders})
        """
        for _, _row in _data.iterrows():
            _cursor.execute(insert_query, tuple(_row.values))

        print(f"* {_table_name} was populated.")

    # populate Movies table
    table_name ="Movies"
    insert_data(cursor, table_name, movies_data)


    connection.commit()
    print("Data loading completed!")

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
                connection_timeout=10
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