"""Contains code responsible for creating the database"""

import os
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
                keyword_id INT,
                PRIMARY KEY (movie_id, Keyword_id),
                FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
                FOREIGN KEY (keyword_id) REFERENCES Keywords(keyword_id)
                );""",
        "production_Companies": """
            CREATE TABLE IF NOT EXISTS Production_Companies (
                production_company_id INT PRIMARY KEY,
                production_company_name VARCHAR(100)
                );""",
        "Movies_Production_Companies": """
            CREATE TABLE IF NOT EXISTS Movies_Production_Companies (
                movie_id INT,
                production_company_id INT,
                PRIMARY KEY (movie_id, production_company_id),
                FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
                FOREIGN KEY (production_company_id) REFERENCES Production_Companies(production_company_id)
                );""",
        "Actors": """
            CREATE TABLE IF NOT EXISTS Actors (
                actor_id INT PRIMARY KEY,
                name VARCHAR(64) NOT NULL UNIQUE,
                gender INT CHECK (gender IN (0, 1, 2))
                );""",
        "Movies_Actors": """
            CREATE TABLE IF NOT EXISTS Movies_Actors (
                movie_id INT,
                actor_id INT,
                character_name VARCHAR(255),
                PRIMARY KEY (movie_id, actor_id),
                FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
                FOREIGN KEY (actor_id) REFERENCES Actors(actor_id)
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


