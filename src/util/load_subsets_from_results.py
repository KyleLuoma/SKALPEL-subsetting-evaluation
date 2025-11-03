"""
Copyright 2025 Kyle Luoma

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import pandas as pd
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from NlSqlBenchmark.SchemaObjects import Schema, SchemaTable, TableColumn
from util.StringObjectParser import StringObjectParser 

def load_subsets_from_results(results_filename: str, benchmark: NlSqlBenchmark) -> tuple[dict, list[tuple[Schema, BenchmarkQuestion]]]:
    results_df = pd.read_excel(results_filename)
    results = {
        "database": [],
        "question_number": [],
        "query_filename": [],
        "inference_time": [],
        "prompt_tokens": []
    }
    subsets_questions = []
    for row in results_df.itertuples():
        if benchmark.get_active_schema().database != row.database:
            benchmark.set_active_schema(row.database)
        benchmark.set_active_question_number(row.question_number)
        question = benchmark.get_active_question()
        results["database"].append(row.database)
        results["question_number"].append(row.question_number)
        try:
            results["query_filename"].append(row.query_filename)
        except:
            results["query_filename"].append("")
        results["inference_time"].append(row.inference_time)
        results["prompt_tokens"].append(row.prompt_tokens)
        correct_tables = StringObjectParser.string_to_python_object(row.correct_tables)
        correct_columns = StringObjectParser.string_to_python_object(row.correct_columns)
        extra_tables = StringObjectParser.string_to_python_object(row.extra_tables)
        if type(extra_tables) == str and extra_tables[0] == "{":
            extra_tables = set([t.strip().replace("'", "") for t in extra_tables[1:].split(",")])
        extra_columns = StringObjectParser.string_to_python_object(row.extra_columns)
        if type(extra_columns) == str and extra_columns[0] == "{":
            extra_columns = set([c.strip().replace("'", "") for c in extra_columns[1:].split(",")])

        table_items = []
        for table_name in correct_tables.union(extra_tables):
            column_items = []
            for table_column in correct_columns.union(extra_columns):
                column_table = ".".join(table_column.split(".")[:-1])
                column_name = table_column.split(".")[-1]
                if column_table != table_name:
                    continue
                table_column = TableColumn(name=column_name, data_type="")
                column_items.append(table_column)
            table_items.append(SchemaTable(
                name=table_name,
                columns=column_items,
                primary_keys=[],
                foreign_keys=[]
                ))
        if "SBOD" in question.schema.database:
            pause = True
        schema_subset = Schema(database=row.database, tables=table_items)
        subsets_questions.append((schema_subset, question))
    return results, subsets_questions