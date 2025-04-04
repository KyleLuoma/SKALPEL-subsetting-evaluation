from NlSqlBenchmark.SchemaObjects import (
    Schema, SchemaTable, TableColumn, ForeignKey
)
import json
import os
from itertools import product
import sqlite3


class SchemaDDLGenerator:

    def __init__(self):
        self.type_maps = self._load_type_maps()
        self.available_target_dialects = [k[1] for k in self.type_maps.keys()]
        self.available_source_dialects = [k[0] for k in self.type_maps.keys()]


    def make_sqlite_database(self, schema: Schema, schema_dialect: str, db_path: str):
        if schema_dialect not in self.available_source_dialects:
            raise ValueError(f"Unsupported source dialect: {schema_dialect}")

        ddl = self.make_schema_ddl(
            schema=schema,
            schema_dialect=schema_dialect,
            target_dialect="sqlite"
        )

        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        try:
            cursor.executescript(ddl)
            connection.commit()
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to create SQLite database: {e}")
        finally:
            connection.close()


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
                    if fk.references[0] in table_sequence:
                        table_sequence.append(eval_table.name)
                    else:
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

        ddl = f"CREATE TABLE {table.name} (\n"
        column_definitions = []

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
                
            column_def = f"  {column.name} {target_data_type}"
            if column.name in table.primary_keys:
                column_def += " PRIMARY KEY"
            column_definitions.append(column_def)

        ddl += ",\n".join(column_definitions)

        if table.foreign_keys:
            foreign_key_definitions = []
            for fk in table.foreign_keys:
                fk_columns = ", ".join(fk.columns)
                ref_table, ref_columns = fk.references
                ref_columns_str = ", ".join(ref_columns)
                foreign_key_definitions.append(
                    f"  FOREIGN KEY ({fk_columns}) REFERENCES {ref_table} ({ref_columns_str})"
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
    
    

