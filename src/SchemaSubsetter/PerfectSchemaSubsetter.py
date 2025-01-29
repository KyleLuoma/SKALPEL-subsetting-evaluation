"""
This class of schema subsetter will return the perfect subset (i.e., recall = precision = 1)
"""

from NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from SchemaSubsetter import SchemaSubsetter
from SubsetEvaluator.QueryProfiler import QueryProfiler
import time

class PerfectSchemaSubsetter(SchemaSubsetter.SchemaSubsetter):
    """
    This class of schema subsetter will return the perfect subset (i.e., recall = precision = 1)
    """

    name = "perferct_subsetter"

    def __init__(self):
        super().__init__()                                                                                                                                                                                                                                                                                                                                          
        self.query_profiler = QueryProfiler()



    def get_schema_subset(self, benchmark_question: BenchmarkQuestion) -> Schema:
        full_schema = benchmark_question.schema
        query_identifiers = self.query_profiler.get_identifiers_and_labels(benchmark_question.query)
        query_tables = query_identifiers["tables"]
        query_columns = query_identifiers["columns"]
        schema_subset = Schema(database=full_schema.database, tables=[])
        for table in full_schema.tables:
            if table.name.upper() in query_tables:
                add_table = SchemaTable(
                    name=table.name,
                    columns=[],
                    primary_keys=[],
                    foreign_keys=[]
                )
                for column in table.columns:
                    if column.name.upper() in query_columns:
                        add_table.columns.append(column)
                schema_subset.tables.append(add_table)
        return schema_subset
    

