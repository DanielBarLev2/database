from sshtunnel import SSHTunnelForwarder
import mysql.connector
from config import config as cfg
from src import create_db_script


if __name__ == "__main__":
    create_db_script.download_and_extract_dataset()


# try:
#     # create an SSH tunnel
#     print("Establishing SSH tunnel...")
#     with SSHTunnelForwarder(ssh_address_or_host=cfg.SSH_CONFIG['ssh_address_or_host'],
#                             ssh_username=cfg.SSH_CONFIG['ssh_username'],
#                             ssh_password=None,
#                             remote_bind_address=cfg.SSH_CONFIG['remote_bind_address'],
#                             local_bind_address=cfg.SSH_CONFIG['local_bind_address']) as tunnel:
#
#         print(f"Tunnel established: Local port {tunnel.local_bind_port}")
#
#         try:
#             print("Connecting to MySQL server...")
#             connection = mysql.connector.connect(
#                 host=cfg.DB_CONFIG['host'],
#                 port=tunnel.local_bind_port,
#                 user=cfg.DB_CONFIG['user'],
#                 password=cfg.DB_CONFIG['password'],
#                 database=cfg.DB_CONFIG['database'],
#                 connection_timeout=60
#             )
#             print("Connected to MySQL server")
#         except mysql.connector.Error as err:
#             print(f"MySQL connection error: {err}")
#         finally:
#             if 'connection' in locals() and connection.is_connected():
#                 connection.close()
#                 print("MySQL connection closed")
#
#
# except Exception as e:
#     print(f"An error occurred: {e}")
