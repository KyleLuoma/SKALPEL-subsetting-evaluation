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
from SchemaSubsetter.SchemaSubsetterResult import SchemaSubsetterResult


class SchemaSubsetter:

    name = "abstract"
    uses_gpu = False
    
    def __init__(self, benchmark: NlSqlBenchmark = None):
        self.benchmark = benchmark
        self.device = None
        pass


    def get_schema_subset(
            self, 
            benchmark_question: BenchmarkQuestion
            ) -> SchemaSubsetterResult:
        """
        'Abstract' method for subset generation

        Parameters
        ----------
        question: str
            The NL question to use for determining the required identifiers
        full_schema: Schema
            Full schema representation
        """
        schema = Schema(
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
        return SchemaSubsetterResult(schema_subset=schema)
    

    def preprocess_databases(self) -> dict[str, float]:
        processing_times = {}
        for database in self.benchmark.databases:
            processing_times["database"] = 0.0
        return processing_times
