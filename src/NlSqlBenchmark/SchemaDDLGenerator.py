from NlSqlBenchmark.SchemaObjects import (
    Schema, SchemaTable, TableColumn, ForeignKey
)
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.QueryResult import QueryResult
import json
import decimal
import os
from itertools import product
import sqlite3
from tqdm import tqdm

class SchemaDDLGenerator:

    def __init__(self):
        self.type_maps = self._load_type_maps()
        self.available_target_dialects = [k[1] for k in self.type_maps.keys()]
        self.available_source_dialects = [k[0] for k in self.type_maps.keys()]
        self.identifier_encase = {
            ("mssql", "open"): "[",
            ("mssql", "close"): "]",
            ("sqlite", "open"): "`",
            ("sqlite", "close"): "`"
        }


    def transform_benchmark_to_sqlite(self, benchmark: NlSqlBenchmark, db_path: str, do_only:list = None):
        for db_name in benchmark.databases:
            if do_only != None and db_name not in do_only:
                continue
            open_encase = self.identifier_encase[benchmark.sql_dialect, "open"]
            close_encase = self.identifier_encase[benchmark.sql_dialect, "close"]
            schema = benchmark.get_active_schema(db_name)
            self.make_sqlite_database(schema=schema, schema_dialect=benchmark.sql_dialect, db_path=db_path)
            save_path = db_path + "/" + schema.database + ".sqlite"
            connection = sqlite3.connect(save_path)
            cursor = connection.cursor()

            table_hash: dict[str, SchemaTable] = {table.name: table for table in schema.tables}
            table_order = self._define_table_creation_sequence(schema)
            for table_name in table_order:
                table = table_hash[table_name]
                all_values = benchmark.execute_query(
                    query=f"select * from {open_encase}{table.name}{close_encase}",
                    database=db_name
                )
                if all_values.result_set == None:
                    print("Error at", table.name, all_values.error_message)
                    continue
                columns = list(all_values.result_set.keys())
                if len(columns) == 0:
                    continue
                if len(all_values.result_set[columns[0]]) == 0:
                    continue
                for ix in tqdm(range(0, len(all_values.result_set[columns[0]])), desc=f"Inserting values into {table.name}"):
                    sql = self.make_sqlite_table_insert_statement(table=table)
                    values = []
                    for c in columns:
                        value = all_values.result_set[c][ix]
                        if isinstance(value, decimal.Decimal):
                            value = float(value)
                        values.append(value)
                    try:
                        cursor.execute(
                            sql, values
                        )
                        
                    except sqlite3.Error as e:
                        print(sql)
                        print(values)
                        raise RuntimeError(f"Failed to insert values into SQLite database: {e}")
                connection.commit()
            connection.close()
                                       


    def make_sqlite_database(self, schema: Schema, schema_dialect: str, db_path: str):
        if schema_dialect not in self.available_source_dialects:
            raise ValueError(f"Unsupported source dialect: {schema_dialect}")

        ddl = self.make_schema_ddl(
            schema=schema,
            schema_dialect=schema_dialect,
            target_dialect="sqlite"
        )

        db_path = db_path + "/" + schema.database + ".sqlite"
        print(db_path)

        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        try:
            cursor.executescript(ddl)
            connection.commit()
        except sqlite3.Error as e:
            print(ddl)
            raise RuntimeError(f"Failed to create SQLite database: {e}")
        finally:
            connection.close()


    def make_sqlite_table_insert_statement(self, table: SchemaTable) -> str:
        open_encase = self.identifier_encase["sqlite", "open"]
        close_encase = self.identifier_encase["sqlite", "close"]
        column_names = ", ".join([f"{open_encase}{column.name}{close_encase}" for column in table.columns])
        placeholders = ", ".join(["?" for _ in table.columns])
        insert_statement = f"INSERT INTO {open_encase}{table.name}{close_encase} ({column_names}) VALUES ({placeholders});"
        return insert_statement


    def make_schema_ddl(self, schema: Schema, schema_dialect: str, target_dialect: str) -> str:
        if (
            target_dialect not in self.available_target_dialects 
            or schema_dialect not in self.available_source_dialects
            ):
            raise ValueError(
                f"Unsupported dialects. Source dialect: {schema_dialect}, Target dialect: {target_dialect}"
            )
        table_hash: dict[str, SchemaTable] = {table.name: table for table in schema.tables}
        table_order = self._define_table_creation_sequence(schema)
        table_ddl = []
        for t_name in table_order:
            table_ddl.append(self._make_create_table_statement(
                table=table_hash[t_name],
                source_sql_dialect=schema_dialect,
                target_sql_dialect=target_dialect
            ))
        return "\n".join(table_ddl)


    def _define_table_creation_sequence(self, schema: Schema) -> list[str]:
        table_sequence: list[str] = []
        remaining_tables: list[SchemaTable] = []
        for table in schema.tables:
            if table.foreign_keys == None or len(table.foreign_keys) == 0:
                table_sequence.append(table.name)
            else:
                remaining_tables.append(table)
        if len(table_sequence) == 0:
            raise ValueError("No tables without foreign keys found. Cannot define table creation sequence.")
        while len(remaining_tables) > 0:
            eval_tables = remaining_tables.copy()
            remaining_tables = []
            for eval_table in eval_tables:
                for fk in eval_table.foreign_keys:
                    if fk.references[0] in table_sequence and eval_table.name not in table_sequence:
                        table_sequence.append(eval_table.name)
                    elif eval_table.name not in table_sequence:
                        remaining_tables.append(eval_table)
        return table_sequence

    

    def _make_create_table_statement(
            self, 
            table: SchemaTable, 
            source_sql_dialect: 
            str, target_sql_dialect: str
            ):
        """
        Generates a CREATE TABLE statement for the given table, translating column types
        from the source SQL dialect to the target SQL dialect.

        Args:
            table (SchemaTable): The table schema definition.
            source_sql_dialect (str): The source SQL dialect (e.g., 'mssql').
            target_sql_dialect (str): The target SQL dialect (e.g., 'sqlite').

        Returns:
            str: The CREATE TABLE SQL statement.
        """
        type_map = self.type_maps.get((source_sql_dialect, target_sql_dialect))
        if not type_map:
            raise ValueError(f"No type map found for {source_sql_dialect} to {target_sql_dialect}")
        
        open_encase = self.identifier_encase[target_sql_dialect, "open"]
        close_encase = self.identifier_encase[target_sql_dialect, "close"]

        ddl = f"CREATE TABLE {open_encase}{table.name}{close_encase} (\n"
        column_definitions = []
        pk_columns = []
        for column in table.columns:
            if "(" in column.data_type and ")" in column.data_type:
                source_type, type_args = self._handle_source_type_with_parens(column.data_type)
            else:
                source_type = column.data_type.upper()
                type_args = None

            target_data_type: str = type_map.get(source_type, column.data_type.upper())

            if type_args != None and "(n" in target_data_type and ")" in target_data_type:
                base_target_type = target_data_type.split("(")[0]
                target_data_type = base_target_type + f"({type_args})"
                
            column_def = f"  {open_encase}{column.name}{close_encase} {target_data_type}"
            if column.name in table.primary_keys:
                pk_columns.append(column.name)
            column_definitions.append(column_def)

        ddl += ",\n".join(column_definitions)

        if len(pk_columns) > 0:
            pk_definition = ", ".join([f"{open_encase}{c}{close_encase}" for c in pk_columns])
            ddl += f",\n  PRIMARY KEY ({pk_definition})"

        if table.foreign_keys:
            foreign_key_definitions = []
            for fk in table.foreign_keys:
                fk_columns = ", ".join([f"{open_encase}{c}{close_encase}" for c in fk.columns])
                ref_table, ref_columns = fk.references
                if type(ref_columns) == str:
                    ref_columns = [ref_columns]
                ref_columns_str = ", ".join([f"{open_encase}{c}{close_encase}" for c in ref_columns])
                foreign_key_definitions.append(
                    f"  FOREIGN KEY ({fk_columns}) REFERENCES {open_encase}{ref_table}{close_encase} ({ref_columns_str})"
                )
            ddl += ",\n" + ",\n".join(foreign_key_definitions)

        ddl += "\n);"
        return ddl
        

    def _handle_source_type_with_parens(self, source_type: str) -> tuple[str, str]:
        base_type, args = source_type.split('(', 1)
        base_type = base_type.upper()
        args = args.rstrip(')')
        if ',' in args:
            return (f"{base_type}(n,m)", args.strip())
        else:
            return (f"{base_type}(n)", args.strip())


    def _load_type_maps(self) -> dict[tuple, dict]:
        type_maps = {}
        artifacts_dir = './src/NlSqlBenchmark/ddl_generator_artifacts'
        for filename in os.listdir(artifacts_dir):
            from_sql = filename.split("_")[0]
            to_sql = filename.split("_")[1]
            if filename.endswith('.json'):
                file_path = os.path.join(artifacts_dir, filename)
            with open(file_path, 'r') as file:
                type_maps[(from_sql, to_sql)] = json.load(file)
        return type_maps
    
    

