# Data-Base Systems Final Project


## Dependencies
> This codebase was developed using Python 3.11.11

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

Together, containing almost 10,000 records.

> kaggle.json must be placed in /.kaggle directory
 
This is a licence to use the dataset.

## Run
first, downloads kaggle data set into the project folder.
Next, creates a ssh tunnel and establishes connection to mySQL server using config.py configurations.
After a successful connection, the script follows this flow:
1. Creates tables in sql server (if not exist already).
2. Loads data from .csv to sql server using cursor object (only if the tables are completely empty). this might take a long time.
3. To test small data insertion, consider adding "df.head(limit).iterrows()":
```python
    for _, row in df.iterrows():
        try:
            cursor.execute(insert_row, tuple(row.astype(object).values))
        except Exception as e:
            print(e)
    print(f"* {table_name} was populated.")
```
At api_data_retrieve.py(insert_data) for reduced upload time and db evaluation.

4. END (for now, up next: calling sql queries from queries_db_script.py]