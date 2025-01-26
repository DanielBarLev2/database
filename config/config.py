import os


DATASET_NAME = "tmdb/tmdb-movie-metadata"
DOWNLOAD_PATH = os.path.join(os.getcwd(), "data")

SSH_CONFIG = {
    "ssh_address_or_host": ("nova.cs.tau.ac.il", 22),
    "ssh_username": "danielbarlev",
    "ssh_password": None,  # SSH key is used instead of a password
    "remote_bind_address": ("mysqlsrv1.cs.tau.ac.il", 3306),
    "local_bind_address": ("127.0.0.1", 3305)
}

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3305,
    "user": "danielbarlev",
    "password": "dan79705",
    "database": "danielbarlev"
}