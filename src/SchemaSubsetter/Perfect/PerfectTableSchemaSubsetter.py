"""
This class of schema subsetter will return a subset with only required tables, 
and all columns for each required table
"""

from NlSqlBenchmark import NlSqlBenchmark
from SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.SchemaSubsetterResult import SchemaSubsetterResult
from SubsetEvaluator.QueryProfiler import QueryProfiler
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion


class PerfectTableSchemaSubsetter(SchemaSubsetter.SchemaSubsetter):
    """
    This class of schema subsetter will return a subset with only required tables, 
    and all columns for each required table
    """

    name = "perfect_table_subsetter"

    def __init__(self, benchmark: NlSqlBenchmark = None):
        super().__init__()
        self.query_profiler = QueryProfiler()
        self.name = PerfectTableSchemaSubsetter.name



    def get_schema_subset(self, benchmark_question: BenchmarkQuestion) -> Schema:
        correct_query = benchmark_question.query
        query_identifiers = self.query_profiler.get_identifiers_and_labels(correct_query, dialect=benchmark_question.query_dialect)
        query_tables = query_identifiers["tables"]
        schema_subset = Schema(database=benchmark_question.schema.database, tables=[])
        for table in benchmark_question.schema.tables:
            if table.name.upper() in query_tables:
                add_table = SchemaTable(
                    name=table.name,
                    columns=table.columns,
                    primary_keys=[],
                    foreign_keys=[]
                )
                schema_subset.tables.append(add_table)
        return SchemaSubsetterResult(
            schema_subset=schema_subset,
            prompt_tokens=0
            )
    