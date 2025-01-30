# Data-Base Systems Final Project


## Dependencies
> This codebase was developed using Python 3.11.11 (using Conda interpreter)

###### Run: 
```bash
pip install -r  requirments.txt
````
###### From IDE's terminal, run (keep open for the entire working session):
```bash
ssh -L 3305:mysqlsrv1.cs.tau.ac.il:3306 <username>@nova.cs.tau.ac.il 
```

#### Ensure you are connected to TAU's VPN!

## Data-Set
The tmdb data-set from kaggle.com was selected for this project.
It is spread across two csv files, read from Kaggle's API.
1. tmdb_5000_credits.csv.
2. tmdb_5000_movies.csv.

Together, containing almost 10,000 nested json records.

> kaggle.json must be placed in /.kaggle directory
 
This is a licence to use the dataset.


## Run
Execute main.py (root) without additional arguments. 

```bash
python main.py 
```
It performs:
1. Downloads kaggle data set into the project folder.
2. creates a ssh tunnel and establishes connection to mySQL server using config.py configurations.

After a successful connection, the script preforms:
1. Creates tables and indexing in sql server (if not exist already).
2. Loads data from .csv to sql server using cursor object (only if the tables are completely empty).
   1. To reload data into the data-set, the former tables must be deleted:
   ```python
    from src.create_db_script import drop_all_tables
    drop_all_tables(cursor, connection)
    ```
   2. This might take a long time. Therefore, try to load small portion first by changing the following:
    ```python
        # @ api_data_retrieve.py > insert_data()
        for _, row in df.head(LIMIT).iterrows(): # <<< add "head(LIMIT)" here
            try:
                cursor.execute(insert_row, tuple(row.astype(object).values))
            except Exception as e:
                print(e)
        print(f"* {table_name} was populated.")
    ```
3. Executes 5 queries from queries_db_script.py using queries_execution.py. Then, prints the outputs in CMD with small explanation about the query.

> Warning: Loading the tables into the database typically takes around 2 hours. Please be cautious to avoid accidentally dropping the tables and having to reload the data.