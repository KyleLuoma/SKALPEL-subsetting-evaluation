from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
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