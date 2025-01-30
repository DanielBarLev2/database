# Handles data insertion.
import json
import pandas as pd
from tqdm import tqdm
from config import config as cfg


def load_data_to_database(cursor, connection):
    """
    Loads and processes data into a database by reading datasets, transforming them into appropriate
    formats, and inserting the data into related database tables.

    The function handles various entities such as Movies, Genres, Keywords, Production Companies, and
    Actors. It processes the relationships between these entities, ensuring proper linkage within the
    database. The function utilizes helper methods for specific tasks like column processing, data
    insertion, and relationship management. Upon completion, the changes are committed to the database.

    :param cursor: Database cursor object used to execute SQL commands.
    :param connection: Database connection object to commit changes or rollback in case of failures.
    :return: None
    """
    print("loading database schema... (this might take a while :|)")

    # load datasets
    movies_data = pd.read_csv(cfg.MOVIE_DATA_PATH)
    credits_data = pd.read_csv(cfg.CREDITS_DATA_PATH)

    # Process and insert Movies data
    movies_df = movies_data.copy()
    movies_df.rename(columns={'id': 'movie_id'}, inplace=True)  # dataset and sql-table first column name mismatch.
    movies_df = movies_df[get_table_columns(cursor, "Movies")]
    insert_data(cursor, "Movies", movies_df, connection)

    # Process and insert Genres data
    genre_df = process_json_column(movies_data, "genres")
    genre_df.columns = get_table_columns(cursor, "Genres")
    genre_df = genre_df.drop_duplicates(subset=['genre_id'])
    insert_data(cursor, "Genres", genre_df, connection)

    # Insert movie-genre relationships
    insert_foreign_data(cursor, movies_data, 'id', 'genres', "Movies_Genres", connection)

    # Process and insert Keywords data
    keyword_df = process_json_column(movies_data, "keywords")
    keyword_df.columns = get_table_columns(cursor, "Keywords")
    keyword_df = keyword_df.drop_duplicates(subset=['keyword_id'])
    insert_data(cursor, "Keywords", keyword_df, connection)

    # Insert movie-keyword relationships
    insert_foreign_data(cursor, movies_data, 'id', 'keywords', "Movies_Keywords", connection)

    # Insert production-companies relationships
    production_companies_df = process_json_column(movies_data, "production_companies")

    production_companies_df = production_companies_df.iloc[:,
                              [1, 0] + list(
                                  range(2, len(production_companies_df.columns)))]  # swaps name <-> id columns
    production_companies_df.columns = get_table_columns(cursor, "Production_Companies")
    production_companies_df = production_companies_df.drop_duplicates(subset=['production_company_id'])
    insert_data(cursor, "Production_Companies", production_companies_df, connection)

    # Insert movie-production-company relationships
    insert_foreign_data(cursor=cursor,
                        df=movies_data,
                        column1='id',
                        column2='production_companies',
                        table_name="Movies_Production_Companies",
                        connection=connection)

    # Process and insert Actors data
    actors_df = process_json_column(credits_data, "cast")
    actors_df.rename(columns={'character': 'character_name', 'id': 'actor_id'}, inplace=True)
    actors_df = actors_df[get_table_columns(cursor, "Actors")]
    actors_df = actors_df.drop_duplicates(subset=['actor_id'])
    insert_data(cursor, "Actors", actors_df, connection)

    # Insert movie-actor relationships
    insert_foreign_data(cursor, credits_data, 'movie_id', 'cast', "Movies_Actors", connection)

    connection.commit()
    print("All data loading completed!")


def insert_data(cursor, table_name, df, connection):
    """
   Inserts data into a specified table if it is not already populated.

   :param connection: connection to database.
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

    # Wrap the DataFrame in tqdm for a progress bar
    for index, row in tqdm(df.iterrows(), total=len(df), desc=f"Inserting into {table_name}"):
        try:
            cursor.execute(insert_row, tuple(row.astype(object).values))
        except Exception as e:
            print(f"\nError inserting row {index} into {table_name}: {e}")
            print(f"Query: {insert_row}")
            print(f"Row values: {tuple(row.astype(object).values)}")

    connection.commit()
    print(f"* {table_name} was populated.")


def insert_foreign_data(cursor, df, column1, column2, table_name, connection):
    """
    Inserts foreign key relationships from JSON data into a specified table.
    ### Uses insert_data to insert foreign key relationships.
    :param connection: connection to database.
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

    insert_data(cursor=cursor, table_name=table_name, df=pairs, connection=connection)


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
