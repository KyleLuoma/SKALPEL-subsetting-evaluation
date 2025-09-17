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
from SchemaSubsetter.SchemaSubsetterResult import SchemaSubsetterResult
from SubsetEvaluator.QueryProfiler import QueryProfiler

import time
import pickle

class PerfectSchemaSubsetter(SchemaSubsetter.SchemaSubsetter):
    """
    This class of schema subsetter will return the perfect subset (i.e., recall = precision = 1)
    """

    name = "perfect_subsetter"

    def __init__(self, benchmark: NlSqlBenchmark = None):
        super().__init__()
        self.query_profiler = QueryProfiler()
        self.name = PerfectSchemaSubsetter.name
        self.benchmark = benchmark



    def get_schema_subset(self, benchmark_question: BenchmarkQuestion, load_from_cache: bool = False) -> SchemaSubsetterResult:
        if load_from_cache:
            recovery_filename = f"./.subsetting_recovery/{self.name}/subsetting_recovery_{self.name}_{self.benchmark.name}_{benchmark_question.schema_naturalness}_{benchmark_question.schema.database}_{benchmark_question.question_number}_oracle.pkl"
            try:
                with open(recovery_filename, "rb") as load_file:
                    loaded_result: dict = pickle.loads(load_file.read())
                    assert len(loaded_result["subset"].tables) > 0
                    return loaded_result["subset"]
            except FileNotFoundError as e:
                print(recovery_filename, "does not exist.")
                pass
            except AssertionError as e:
                print(e, recovery_filename, loaded_result["subset"])
                print(benchmark_question.query_dialect, benchmark_question.query)
                pass
        full_schema = benchmark_question.schema
        query_identifiers = self.query_profiler.get_identifiers_and_labels(
            query=benchmark_question.query,
            dialect=benchmark_question.query_dialect,
            include_brackets=False
            )
        # print("DEBUG get_schema_subset query_identifiers:", query_identifiers)
        query_tables = query_identifiers["tables"]
        query_tables += [t.split(".")[-1] for t in query_tables if "." in t]
        query_columns = query_identifiers["columns"]
        schema_subset = Schema(database=full_schema.database, tables=[])

        wildcard_tables = []
        for table in query_identifiers["tables"]:
            if table[-1] == "*":
                wildcard_tables.append(table.replace("*", ""))
        

        for table in full_schema.tables:
            in_query_tables = False
            if table.name.upper() in query_tables:
                in_query_tables = True
            if len(table.name.split(".")) > 1 and table.name.split(".")[-1].upper() in query_tables:
                in_query_tables = True

            if in_query_tables:
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

        for wc_table in wildcard_tables:
            for sch_table in full_schema.tables:
                if wc_table in sch_table.name.upper():
                    add_table = SchemaTable(
                        name=sch_table.name,
                        columns=[],
                        primary_keys=[],
                        foreign_keys=[]
                    )
                    for column in sch_table.columns:
                        if column.name.upper() in query_columns:
                            add_table.columns.append(column)
                    schema_subset.tables.append(add_table)
                    
        return SchemaSubsetterResult(
            schema_subset=schema_subset,
            prompt_tokens=0
            )
    

