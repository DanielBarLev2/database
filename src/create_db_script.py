"""Contains code responsible for creating the database"""

import os
import json
import kaggle
import mysql.connector
from sshtunnel import SSHTunnelForwarder

from config import config as cfg


def download_and_extract_dataset():
    """
    Creates a directory named /data and downloads and unzip the dataset from kaggle.

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
        Initializes the database schema from the queries in create_table_queries.
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
        print(f"* {table_name} table was created successfully")

    print("Database schema created successfully!")



def load_data_to_database(cursor):
    """
        @todo: Load the dataset into the database.
    """
    pass



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

        print(f"Tunnel established: Local port {tunnel.local_bind_port}")

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

            print("connected to MySQL server")
            cursor = connection.cursor()

            create_database_schema(cursor)
            load_data_to_database(cursor)

        except mysql.connector.Error as err:
            print(f"MySQL connection error: {err}")

        finally:
            if 'connection' in locals() and connection.is_connected():
                connection.close()
                print("MySQL connection closed")