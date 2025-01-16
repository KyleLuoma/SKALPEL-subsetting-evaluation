from SchemaSubsetter import SchemaSubsetter
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion


def get_schema_subset_test():
    ss = SchemaSubsetter.SchemaSubsetter(benchmark=None)
    result = ss.get_schema_subset(BenchmarkQuestion(
        question="Why foo bar?",
        schema=Schema(database="foo", tables=[]),
        question_number=0,
        query=""
    ))
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