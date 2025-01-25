# Contains code responsible for creating the database.

import os
import zipfile
import kaggle
import pandas as pd
import json
import mysql.connector
from kaggle.api.kaggle_api_extended import KaggleApi
from config import config as cfg


def download_and_extract_dataset():
    """
    Downloads and extracts a dataset from Kaggle.

    This function performs the following steps:
       1. Creates a directory at the configured download path if it does not
          exist.
       2. Authenticates the user with the Kaggle API.
       3. Downloads and extracts the specified dataset into the created
          directory.

    :raises OSError: Raised if the target directory already exists.
    :raises kaggle.rest.ApiException: Raised if an error occurs when interacting
                                      with the Kaggle API.
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

def main():
    download_and_extract_dataset()


if __name__ == "__main__":
    main()