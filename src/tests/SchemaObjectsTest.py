from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from NlSqlBenchmark.snails.SnailsNlSqlBenchmark import SnailsNlSqlBenchmark
from NlSqlBenchmark.spider2.Spider2NlSqlBenchmark import Spider2NlSqlBenchmark
import pickle
import os


def schema_pickle_test():
    schema = Schema(
        database="pickle_me",
        tables=[
            SchemaTable(
                name="dill",
                columns=
            [
                TableColumn(name="id", data_type="INTEGER"),
                TableColumn(name="name", data_type="TEXT"),
                TableColumn(name="value", data_type="REAL")
            ],
            primary_keys=[],
            foreign_keys=[ForeignKey(columns=["value"], references=("relish", ["id"]))]
            )
        ]
    )
    with open("schema.pkl", "wb") as f:
        pickle.dump(schema, f)
    with open("schema.pkl", "rb") as f:
        loaded_schema = pickle.load(f)
    print(loaded_schema)
    os.remove("schema.pkl")
    return True



def schematable_as_ddl_test():
    table = SchemaTable(
        name="test_table",
        columns=[
            TableColumn("colA", "int"),
            TableColumn("colB", "varchar(3)")
        ],
        primary_keys=["colA"],
        foreign_keys=[ForeignKey(["colB"], ("t2", ["colC"]))]
        )
    return table.as_ddl() == """CREATE TABLE test_table (
  colA int PRIMARY KEY,
  colB varchar(3),
  FOREIGN KEY (colB) REFERENCES t2 (colC)
);"""


def schema_col_count_test():
    snails = SnailsNlSqlBenchmark(sql_dialect="sqlite", db_host_profile="sqlite")
    schema = snails.get_active_schema(database="NTSB")
    print(schema.get_column_count())
    return schema.get_column_count() == 1161


def schema_as_bird_json_format_test():
    schema = Schema(
        database="pickle_me",
        tables=[
            SchemaTable(
                name="dill",
                columns=
            [
                TableColumn(name="id", data_type="INTEGER"),
                TableColumn(name="name", data_type="TEXT"),
                TableColumn(name="value", data_type="REAL")
            ],
            primary_keys=["id"],
            foreign_keys=[ForeignKey(columns=["value"], references=("relish", ["id"]))]
            ),
            SchemaTable(
                name="relish",
                columns=[
                    TableColumn(name="id", data_type="INTEGER")
                ]
            )
        ]
    )
    schema_dict = schema.as_bird_json_format()
    return schema_dict == {
        'db_id': 'pickle_me', 
        'table_names': ['dill', 'relish'], 
        'table_names_original': ['dill', 'relish'], 
        'column_names': [[-1, '*'], [0, 'id'], [0, 'name'], [0, 'value'], [1, 'id']], 
        'column_types': ['INTEGER', 'TEXT', 'REAL', 'INTEGER'], 
        'column_names_original': [[-1, '*'], [0, 'id'], [0, 'name'], [0, 'value'], [1, 'id']], 
        'primary_keys': [0], 
        'foreign_keys': [[2, 3]]}