import os


DATASET_NAME = "tmdb/tmdb-movie-metadata"
DOWNLOAD_PATH = os.path.join(os.getcwd(), "data")

MOVIE_DATA_PATH = os.path.join(DOWNLOAD_PATH, "tmdb_5000_movies.csv")
CREDITS_DATA_PATH = os.path.join(DOWNLOAD_PATH, "tmdb_5000_credits.csv")


DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3305,
    "user": "annap",
    "password": "annap123",
    "database": "annap"
}