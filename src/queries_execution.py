""" Includes the main function and provides example usages of your queries from queries db script.py."""

import pandas as pd
import mysql
import mysql.connector

from config import config as cfg
from sshtunnel import SSHTunnelForwarder
from src.api_data_retrieve import load_data_to_database
from src.queries_db_script import query_1, query_2, query_3, query_4, query_5, query_6
from src.create_db_script import download_and_extract_dataset, create_database_schema, drop_all_tables


def execute_query(connection, query_func, *args, max_width=32):
    """
    Executes the given query function, prints its documentation,
    and displays the first results in a DataFrame, including column names.
    :param connection: connection to database.
    :param query_func: query to execute.
    :param max_width: maximum width of DataFrame columns.
    :param args: arguments to pass to query_func.
    """
    try:
        # Print function docstring (query documentation)
        if query_func.__doc__:
            docstring = query_func.__doc__.strip()
        else:
            docstring = "No documentation available."
        print(f"\nQuery Documentation for {query_func.__name__}:\n{docstring}\n")

        results, column_names = query_func(connection, *args)

        if results:
            df = pd.DataFrame(results, columns=column_names)

            # custom formatter for truncating and padding cells
            def custom_formatter(x):
                x = str(x)
                if len(x) > max_width:
                    return x[:max_width - 3] + '...'
                return x.ljust(max_width)

            formatters = {}
            for col in df.columns:
                formatters[col] = custom_formatter

            formatted_column_names = []
            for col in df.columns:
                formatted_column_names.append(custom_formatter(col))

            print(''.join(formatted_column_names))

            print(df.to_string(index=False, formatters=formatters, header=False))
        else:
            print(f"\nNo results found for {query_func.__name__}.")

    except Exception as e:
        print(f"Error executing {query_func.__name__}: {e}")


def main():
    """
    This function serves as the central hub of the script, managing multiple critical tasks:
    Downloading and extracting the dataset.
    Connecting to a MySQL database.
    Updating the database schema.
    Populating the database.
    Running various SQL queries.
    """

    download_and_extract_dataset()

    try:
        print("connecting to MySQL server...")
        connection = mysql.connector.connect(
            host=cfg.DB_CONFIG['host'],
            port=cfg.DB_CONFIG['port'],
            user=cfg.DB_CONFIG['user'],
            password=cfg.DB_CONFIG['password'],
            database=cfg.DB_CONFIG['database'],
            connection_timeout=60
        )

        cursor = connection.cursor(prepared=True)   # Create a MySQLCursorPrepared cursor to enable prepared statements

        # inp = input("drop all tables? (yes)")
        # if inp.lower() == "yes":
        #     drop_all_tables(cursor, connection)

        # create_database_schema(cursor)
        # load_data_to_database(cursor, connection)

        try:
            execute_query(connection, query_1, "future galaxy")
            execute_query(connection, query_2, "Skarsgard")
            execute_query(connection, query_3, "Comedy")
            execute_query(connection, query_4, "Drama")
            execute_query(connection, query_5)
            execute_query(connection, query_6, "The Hitchhiker's Guide to the Galaxy")
        except mysql.connector.Error as error:
            print("Failed to execute query: {}".format(error))

    except mysql.connector.Error as err:
        print(f"MySQL connection error: {err}")

    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("MySQL connection closed")


if __name__ == '__main__':
    main()
