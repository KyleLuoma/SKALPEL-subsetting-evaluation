from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark, BenchmarkQuestion
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
import pandas as pd
from util.StringObjectParser import StringObjectParser

class NlSqlEvaluator:

    def __init__(
            self,
            benchmark: NlSqlBenchmark,
            subset_df: pd.DataFrame = None
            ):
        self.benchmark = benchmark
        self.subsets = self._get_subsets_from_subset_df(subset_df=subset_df)



    def _get_subsets_from_subset_df(self, subset_df: pd.DataFrame) -> list[BenchmarkQuestion]:
        question_list = []
        for row in subset_df.itertuples():
            correct_tables: set = StringObjectParser.string_to_python_object(row.correct_tables, True)
            missing_tables: set = StringObjectParser.string_to_python_object(row.missing_tables, True)
            extra_tables: set = StringObjectParser.string_to_python_object(row.extra_tables, True)
            correct_columns: set = StringObjectParser.string_to_python_object(row.correct_columns, True)
            missing_columns: set = StringObjectParser.string_to_python_object(row.missing_columns, True)
            extra_columns: set = StringObjectParser.string_to_python_object(row.extra_columns, True)
            subset_tables = correct_tables.union(missing_tables).union(extra_tables)
            subset_columns = correct_columns.union(missing_columns).union(extra_columns)

            self.benchmark.set_active_schema(row.database)
            self.benchmark.set_active_question_number(row.question_number)
            schema = self.benchmark.get_active_schema()
            subset_schema = Schema(
                dabase=schema.database, tables=[]
            )
            for table in schema.tables:
                if table.name not in subset_tables:
                    continue
                subset_table = SchemaTable(name=table.name, columns=[])
                for column in table.columns:
                    if f"{table.name}.{column.name}" not in subset_columns:
                        continue
                    subset_table.columns.append(column)
                subset_schema.append(subset_table)
            subset_question = self.benchmark.get_active_question()
            subset_question.schema = subset_schema
            question_list.append(subset_question)
        return question_list



