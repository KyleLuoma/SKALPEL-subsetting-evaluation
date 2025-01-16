"""
This class of schema subsetter will return a subset with only required tables, 
and all columns for each required table
"""

from NlSqlBenchmark import NlSqlBenchmark
from SchemaSubsetter import SchemaSubsetter
from QueryProfiler import QueryProfiler
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
    def __init__(self, benchmark: NlSqlBenchmark.NlSqlBenchmark):
        super().__init__(benchmark)
        self.question_lookup = {
            q["question"]: q["question_number"] for q in self.benchmark
        }
        self.query_profiler = QueryProfiler()



    def get_schema_subset(self, benchmark_question: BenchmarkQuestion) -> Schema:
        question_number = self.question_lookup[benchmark_question.question]
        correct_query = self.benchmark.active_database_queries[question_number]
        query_identifiers = self.query_profiler.get_identifiers_and_labels(correct_query)
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
        return schema_subset
    