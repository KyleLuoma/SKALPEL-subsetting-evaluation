"""
Super class for schema subsetting methods we evaluate in the SKALPEL project
"""

from NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn
)

class SchemaSubsetter:

    name = "abstract"
    
    def __init__(
            self,
            benchmark: NlSqlBenchmark.NlSqlBenchmark
            ):
        self.benchmark = benchmark

    def get_schema_subset(
            self, 
            question: str, 
            full_schema: Schema
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