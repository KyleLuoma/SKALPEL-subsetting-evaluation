import pandas as pd
import sqlite3
import json

def get_tables_and_columns_from_sqlite_db(
        db_name: str, 
        append_col_types: bool = True,
        table_list: list = None,
        uppercase: bool = False,
        schema: str = "dbo",
        db_list_file: str = '.local/sqlite_dbinfo.json'
        ) -> dict:
    """
    Returns a dictionary of tables and columns from a SQLite database.

    Parameters
    ----------
    db_name : str
        The name of the database to use for the schema.
    append_col_types : bool, optional
        Whether to append the column types to the column names. The default is True.
    table_list : list, optional
        A list of tables to limit the request to. The default is None. If a value is passed, only the columns of the tables in the list will be returned.
    uppercase : bool, optional
        Whether to return table and column names in uppercase. The default is False.
    schema : str, optional
        Not used for SQLite but kept for compatibility. Defaults to "dbo".
    db_list_file : str, optional
        The path to the database list file. Defaults to '.local/sqlite_dbinfo.json'.

    Returns
    -------
    dict
        A dictionary where keys are table names and values are lists of column names (with or without types).
    """
    with open(db_list_file, 'r') as f:
        db_list = json.load(f)

    db_path = None
    for entry in db_list:
        if entry['database'] == db_name:
            db_path = entry['db_path']
            break

    if not db_path:
        raise ValueError(f"Database {db_name} not found in {db_list_file}")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    if table_list is not None:
        table_list = [t.upper() if uppercase else t for t in table_list]
        tables = [t for t in tables if t in table_list]

    tables_and_columns = {}
    for table in tables:
        cursor.execute(f"PRAGMA table_info(`{table}`);")
        columns = cursor.fetchall()
        column_list = []
        for column in columns:
            column_name = column[1]
            column_type = column[2]
            if uppercase:
                column_name = column_name.upper()
            if append_col_types and column_type:
                column_list.append(f"{column_name} {column_type}")
            else:
                column_list.append(column_name)
        tables_and_columns[table.upper() if uppercase else table] = column_list

    conn.close()
    return tables_and_columns



def do_query(
        query: str, 
        database_name: str, 
        debug: bool = False, 
        convert_bytes_to_str: bool = False,
        db_list_file: str = ".local/sqlite_dbinfo.json"
        ) -> dict:
    """
    Executes a SQL query on a specified SQLite database and returns the result as a dictionary.

    Parameters
    ----------
    query : str
        The SQL query to execute.
    database_name : str
        The name of the database to connect to.
    debug : bool, optional
        If True, prints debug information. Defaults to False.
    convert_bytes_to_str : bool, optional
        If True, converts byte values to strings. Defaults to False.
    db_list_file : str, optional
        The path to the database list file. Defaults to ".local/sqlite_dbinfo.json".

    Returns
    -------
    dict
        A dictionary where keys are column names and values are lists of column values.
    """

    with open(db_list_file, 'r') as f:
        db_list = json.load(f)

    db_path = None
    for entry in db_list:
        if entry['database'] == database_name:
            db_path = entry['db_path']
            break

    if not db_path:
        raise ValueError(f"Database {database_name} not found in {db_list_file}")

    if debug:
        print(f"Connecting to SQLite database at {db_path}")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if debug:
        print(f"Executing query: {query}")

    cursor.execute(query)
    columns = [description[0] for description in cursor.description]

    for i in range(len(columns)):
        if columns[i] in columns[:i]:
            columns[i] = f"{columns[i]}_{i}"

    result_dict = {column: [] for column in columns}

    for row in cursor.fetchall():
        for i, value in enumerate(row):
            if convert_bytes_to_str and isinstance(value, bytes):
                value = value.decode("utf-8")
            result_dict[columns[i]].append(value)

    cursor.close()
    conn.close()

    if debug:
        print("Query execution completed.")

    return result_dict