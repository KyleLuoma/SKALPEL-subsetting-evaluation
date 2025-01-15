from SchemaSubsetter import SchemaSubsetter
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn
)

def get_schema_subset_test():
    ss = SchemaSubsetter.SchemaSubsetter(benchmark=None)
    result = ss.get_schema_subset(
        question="why foo bar?",
        full_schema={}
    )
    return result == Schema(
            database="database1",
            tables=[
                SchemaTable(
                    name="table1",
                    columns=[
                        TableColumn(name="column1", data_type="int")
                    ],
                    primary_keys=[],
                    foreign_keys=[]
                )
            ]
        )