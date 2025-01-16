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
from QueryProfiler import QueryProfiler

class PerfectSchemaSubsetter(SchemaSubsetter.SchemaSubsetter):
    """
    This class of schema subsetter will return the perfect subset (i.e., recall = precision = 1)
    """
    def __init__(self, benchmark: NlSqlBenchmark.NlSqlBenchmark):
        super().__init__(benchmark)
        question_number = self.benchmark.active_question_no
        self.question_lookup = {
            q["question"]: q["question_number"] for q in self.benchmark.set_active_question_number(0)
        }
        self.benchmark.set_active_question_number(question_number)
        self.query_profiler = QueryProfiler()



    def get_schema_subset(self, benchmark_question: BenchmarkQuestion) -> Schema:
        question = benchmark_question.question
        full_schema = benchmark_question.schema
        
        question_number = self.question_lookup[question]
        self.benchmark.set_active_schema(full_schema["database"])
        correct_query = self.benchmark.active_database_queries[question_number]
        query_identifiers = self.query_profiler.get_identifiers_and_labels(correct_query)
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
    

