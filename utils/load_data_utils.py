import json
from typing import overload

import pandas as pd


def get_table_columns(cursor, table_name):
    """
    Retrieves the column names of a given table.

    :param cursor: Database cursor for executing queries.
    :param table_name: Name of the table.
    :return: A list containing the column names of the table.
    """
    cursor.execute(f"DESCRIBE {table_name}")
    return [_row[0] for _row in cursor.fetchall()]


def handle_missing_values(data):
    """
    Handles missing values in the provided DataFrame.

    :param data: DataFrame containing the data.
    :return: DataFrame with missing values handled.
    """
    for column in data.columns:
        if column == 'release_date':

            data[column] = pd.to_datetime(data[column], errors='coerce')
            data[column] = data[column].where(data[column].notnull(), None)

        elif data[column].dtype == 'object':
            data[column].fillna('', inplace=True)

        else:
            data[column].fillna(0, inplace=True)

    return data


def process_json_column(df, column_name):
    """
    Processes a column containing JSON data and converts it into a DataFrame.

    :param df: DataFrame containing the JSON column.
    :param column_name: Name of the column containing JSON data.
    :return: A new DataFrame extracted from the JSON column.
    """
    column_data = []
    for json_str in df[column_name]:
        data = json.loads(json_str)
        data_frame = pd.DataFrame(data)
        column_data.append(data_frame)

    new_df = pd.concat(column_data, ignore_index=True)
    return new_df


def table_exist(cursor, table_name):
    """
    Checks whether a given table exists.

    :param cursor: Database cursor for executing queries.
    :param table_name: Name of the table.
    :return: True if the table exists, False otherwise.
    """
    # count amount of rows in tabel_name
    query = f"SELECT COUNT(*) FROM {table_name};"
    cursor.execute(query)
    row_count = cursor.fetchone()[0]
    if row_count > 0:
        return True
    return False


def insert_data(cursor, table_name, df):
    """
   Inserts data into a specified table if it is not already populated.

   :param cursor: Database cursor for executing queries.
   :param table_name: Name of the table.
   :param df: DataFrame containing data to insert.
   """
    if table_exist(cursor=cursor, table_name=table_name):
        print(f"% {table_name} was already populated.")
        return

    columns = get_table_columns(cursor, table_name)

    df = handle_missing_values(df)

    placeholders = ", ".join(["%s"] * len(columns))
    insert_row = f"""
                INSERT INTO {table_name} ({', '.join(columns)}) 
                VALUES ({placeholders})
                """

    for _, row in df.head(10).iterrows():
        cursor.execute(insert_row, tuple(row.astype(object).values))

    print(f"* {table_name} was populated.")


def insert_foreign_data(cursor, df, column1, column2, table_name):
    """
    Inserts foreign key relationships from JSON data into a specified table.

    :param cursor: Database cursor for executing queries.
    :param df: DataFrame containing the foreign key data.
    :param column1: The primary key column in the main table.
    :param column2: The JSON column containing foreign key references.
    :param table_name: Name of the table to insert relationships.
    """
    pairs = []
    for _, row in df.iterrows():
        id1 = row[column1]
        json_row = json.loads(row[column2])
        for vals in json_row:
            id2 = vals['id']
            pairs.append((id1, id2))

    pairs = pd.DataFrame(pairs, columns=get_table_columns(cursor, table_name)[:2])

    # edge case: Movies_Actors has 3 attributes
    if table_name == "Movies_Actors":
        pairs['character_name'] = process_json_column(df, "cast")['character']

    insert_data(cursor=cursor, table_name=table_name, df=pairs)

