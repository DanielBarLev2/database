from sshtunnel import SSHTunnelForwarder
import mysql.connector
from config import config as cfg
from src import create_db_script


if __name__ == "__main__":
    create_db_script.main()
