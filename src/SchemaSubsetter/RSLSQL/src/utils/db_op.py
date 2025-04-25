import sqlite3
from SchemaSubsetter.RSLSQL.src.configs.config import dev_databases_path

# Skalpel imports
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory

def connect_to_db(db_name):
    return sqlite3.connect(dev_databases_path+'/' + db_name + f'/{db_name}.sqlite')

def get_all_table_names(db_name):
    conn = connect_to_db(db_name)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence';")
    table_names = cursor.fetchall()

    conn.close()

    return [name[0] for name in table_names]

# Skalpel mod:
def get_all_table_names(db_name):
    bm_fact = NlSqlBenchmarkFactory()
    bm_name = bm_fact.lookup_benchmark_by_db_name(db_name=db_name)
    bm = bm_fact.build_benchmark(bm_name)
    schema = bm.get_active_schema(database=db_name)
    return [table.name for table in schema.tables]

def get_all_column_names(db_name, table_name):
    conn = connect_to_db(db_name)
    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info('{table_name}');")
    table_info = cursor.fetchall()

    column_names = [column[1] for column in table_info]

    conn.close()

    return column_names

# Skalpel mod:
def get_all_column_names(db_name, table_name):
    bm_fact = NlSqlBenchmarkFactory()
    bm_name = bm_fact.lookup_benchmark_by_db_name(db_name=db_name)
    bm = bm_fact.build_benchmark(bm_name)
    schema = bm.get_active_schema(database=db_name)
    table = schema.get_table_by_name(table_name=table_name)
    return [column.name for column in table.columns]


def get_foreign_key_info(db_name, table_name):
    conn = connect_to_db(db_name)
    cursor = conn.cursor()

    cursor.execute(f"PRAGMA foreign_key_list('{table_name}');")
    foreign_key_info = cursor.fetchall()

    conn.close()

    return foreign_key_info

def get_table_infos(database_name):
    table_list = get_all_table_names(database_name)
    table_str = '#\n# '
    for table in table_list:
        column_list = get_all_column_names(database_name, table)

        column_list = ['`' + column + '`' for column in column_list]

        columns_str = f'{table}(' + ', '.join(column_list) + ')'

        table_str += columns_str + '\n# '

    return table_str


## 外键信息
def get_foreign_key_infos(database_name):
    table_list = get_all_table_names(database_name)

    foreign_str = '#\n# '
    for table in table_list:
        foreign_lists = get_foreign_key_info(database_name, table)

        for foreign in foreign_lists:
            foreign_one = f'{table}({foreign[3]}) references {foreign[2]}({foreign[4]})'
            foreign_str += foreign_one + '\n# '
            # print(foreign_one)

    return foreign_str


# Skalpel mod:
def get_foreign_key_infos(database_name):
    bm_fact = NlSqlBenchmarkFactory()
    bm_name = bm_fact.lookup_benchmark_by_db_name(db_name=database_name)
    bm = bm_fact.build_benchmark(bm_name)
    schema = bm.get_active_schema(database=database_name)
    foreign_str = '#\n#'
    for table in schema.tables:
        if not table.foreign_keys:
            continue
        for fk in table.foreign_keys:
            foreign_one = f'{table.name}({",".join(fk.columns)}) references {fk.references[0]}({",".join(fk.references[1])})'
            foreign_str += foreign_one + '\n# '
    return foreign_str


def get_throw_row_data(db_name):
    # Dynamically load the first three rows of data
    simplified_ddl_data = []
    # Read the database
    mydb = connect_to_db(db_name)  # 链接数据库
    cur = mydb.cursor()
    # Tables
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    Tables = cur.fetchall()  # Tables is a list of tuples
    for table in Tables:
        # Columns
        cur.execute(f"select * from `{table[0]}`")
        col_name_list = [tuple[0] for tuple in cur.description]
        # print(col_name_list)
        db_data_all = []
        # Retrieve the first three rows of data
        for i in range(3):
            db_data_all.append(cur.fetchone())
        # ddls_data
        test = ""
        for idx, column_data in enumerate(col_name_list):
            try:
                test += f"`{column_data}`[{list(db_data_all[0])[idx]},{list(db_data_all[1])[idx]},{list(db_data_all[2])[idx]}],"
            except:
                test = test
        simplified_ddl_data.append(f"{table[0]}({test[:-1]})")
    ddls_data = "# " + ";\n# ".join(simplified_ddl_data) + ";\n"

    return ddls_data


# Skalpel mod
def get_throw_row_data(db_name):
    bm_fact = NlSqlBenchmarkFactory()
    bm_name = bm_fact.lookup_benchmark_by_db_name(db_name=db_name)
    bm = bm_fact.build_benchmark(bm_name)
    simplified_ddl_data = []
    for table in bm.get_active_schema(database=db_name).tables:
        test = ""
        for column in table.columns:
            col_vals = bm.get_sample_values(table_name=table.name, column_name=column.name, num_values=3)
            try:
                test += f"`{column.name}`[{','.join(col_vals)}],"
            except:
                test = test
        simplified_ddl_data.append(f"{table.name}({test[:-1]})")
    ddls_data = "# " + ";\n# ".join(simplified_ddl_data) + ";\n"
    return ddls_data

