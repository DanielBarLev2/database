import json

import numpy as np
import pandas as pd
import mysql.connector

def get_table_columns(cursor, table_name):
    """
    @:return: the column names of a given table.
    """
    cursor.execute(f"DESCRIBE {table_name}")
    return [_row[0] for _row in cursor.fetchall()]


def handle_missing_values(data):
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
    column_data = []
    for json_str in df[column_name]:
        data = json.loads(json_str)
        data_frame = pd.DataFrame(data)
        column_data.append(data_frame)

    new_df = pd.concat(column_data, ignore_index=True)
    return new_df


def table_exist(cursor, table_name):
    """

    :param cursor:
    :param table_name:
    :return: True if the table exists.
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
    Inserts data into a specified table.
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

    for _, row in df.head(100).iterrows():
        cursor.execute(insert_row, tuple(row.astype(object).values))

    print(f"* {table_name} was populated.")


def insert_foreign_data(cursor, df, column1, column2, table_name):
    pairs = []
    for _, row in df.iterrows():
        id1 = row[column1]
        json_row = json.loads(row[column2])
        for vals in json_row:
            id2 = vals['id']
            pairs.append((id1, id2))

    pairs = pd.DataFrame(pairs, columns=get_table_columns(cursor, table_name))

    insert_data(cursor=cursor, table_name=table_name, df=pairs)

