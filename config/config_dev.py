import os


BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Get the directory of config.py
SSH_KEY_PATH = os.path.join(BASE_DIR, "nova.pem")  # Relative path to SSH key

SSH_CONFIG = {
    "ssh_address_or_host": ("nova.cs.tau.ac.il", 22),
    "ssh_username": "annap",
    "ssh_password": None,  # SSH key is used instead of a password
    "ssh_pkey": SSH_KEY_PATH,
    "remote_bind_address": ("mysqlsrv1.cs.tau.ac.il", 3306),
    "local_bind_address": ("127.0.0.1", 3305)
}