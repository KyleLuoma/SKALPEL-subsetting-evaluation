"""
Super class for schema subsetting methods we evaluate in the SKALPEL project
"""

from NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion


class SchemaSubsetter:

    name = "abstract"
    
    def __init__(self):
        pass
    

    def get_schema_subset(
            self, 
            benchmark_question: BenchmarkQuestion
            ) -> Schema:
        """
        'Abstract' method for subset generation

        Parameters
        ----------
        question: str
            The NL question to use for determining the required identifiers
        full_schema: Schema
            Full schema representation
        """
        return Schema(
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