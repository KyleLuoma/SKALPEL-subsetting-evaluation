"""
Class for evaluating a schema subset against the schema identifiers required for a NL-to-SQL question
"""

from SchemaSubsetter.PerfectSchemaSubsetter import PerfectSchemaSubsetter
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from SubsetEvaluation import SubsetEvaluation
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
import time

class SchemaSubsetEvaluator:

    def __init__(self):
        self.subsetter = PerfectSchemaSubsetter()
        pass



    def evaluate_schema_subset(
            self,
            predicted_schema_subset: Schema,
            question: BenchmarkQuestion
            ) -> SubsetEvaluation:
        
        s_time = time.perf_counter()
        # subsetter = PerfectSchemaSubsetter(self.benchmark)

        s_time = time.perf_counter()
        correct_schema_subset = self.subsetter.get_schema_subset(benchmark_question=question)

        all_correct_tables = {table["name"] for table in correct_schema_subset["tables"]}
        all_predicted_tables = {table["name"] for table in predicted_schema_subset["tables"]}

        subset_identifiers = {
            "correct": {"tables": all_correct_tables, "columns": set()},
            "predicted": {"tables": all_predicted_tables, "columns": set()}
        }

        for subset in [["correct", correct_schema_subset], ["predicted", predicted_schema_subset]]:
            for table in subset[1]["tables"]:
                for column in table["columns"]:
                    table_column = f"{table['name']}.{column['name']}"
                    subset_identifiers[subset[0]]["columns"].add(table_column)

        # print(subset_identifiers)

        return SubsetEvaluation(
            total_recall=self.recall(
                correct=subset_identifiers["correct"]["tables"].union(subset_identifiers["correct"]["columns"]),
                predicted=subset_identifiers["predicted"]["tables"].union(subset_identifiers["predicted"]["columns"])
                ),
            total_precision=self.precision(
                correct=subset_identifiers["correct"]["tables"].union(subset_identifiers["correct"]["columns"]),
                predicted=subset_identifiers["predicted"]["tables"].union(subset_identifiers["predicted"]["columns"])
                ),
            total_f1=self.f1(
                correct=subset_identifiers["correct"]["tables"].union(subset_identifiers["correct"]["columns"]),
                predicted=subset_identifiers["predicted"]["tables"].union(subset_identifiers["predicted"]["columns"])
                ),
            table_recall=self.recall(
                correct=subset_identifiers["correct"]["tables"],
                predicted=subset_identifiers["predicted"]["tables"]
                ),
            table_precision=self.precision(
                correct=subset_identifiers["correct"]["tables"],
                predicted=subset_identifiers["predicted"]["tables"]
                ),
            table_f1=self.f1(
                correct=subset_identifiers["correct"]["tables"],
                predicted=subset_identifiers["predicted"]["tables"]
                ),
            column_recall=self.recall(
                correct=subset_identifiers["correct"]["columns"],
                predicted=subset_identifiers["predicted"]["columns"]
                ),
            column_precision=self.precision(
                correct=subset_identifiers["correct"]["columns"],
                predicted=subset_identifiers["predicted"]["columns"]
                ),
            column_f1=self.f1(
                correct=subset_identifiers["correct"]["columns"],
                predicted=subset_identifiers["predicted"]["columns"]
                ),
            correct_tables=self.correct_identifiers(
                correct=subset_identifiers["correct"]["tables"],
                predicted=subset_identifiers["predicted"]["tables"]
            ),
            missing_tables=self.missing_identifiers(
                correct=subset_identifiers["correct"]["tables"],
                predicted=subset_identifiers["predicted"]["tables"]
            ),
            extra_tables=self.extra_identifiers(
                correct=subset_identifiers["correct"]["tables"],
                predicted=subset_identifiers["predicted"]["tables"]
            ),
            correct_columns=self.correct_identifiers(
                correct=subset_identifiers["correct"]["columns"],
                predicted=subset_identifiers["predicted"]["columns"]
            ),
            missing_columns=self.missing_identifiers(
                correct=subset_identifiers["correct"]["columns"],
                predicted=subset_identifiers["predicted"]["columns"]
            ),
            extra_columns=self.extra_identifiers(
                correct=subset_identifiers["correct"]["columns"],
                predicted=subset_identifiers["predicted"]["columns"]
            ),
            subset_table_proportion=self.table_proportion(
                predicted_schema=predicted_schema_subset, full_schema=question.schema
            ),
            subset_column_proportion=self.column_proportion(
                predicted_schema=predicted_schema_subset, full_schema=question.schema
            )
        )
    


    def table_proportion(self, predicted_schema: Schema, full_schema: Schema) -> float:
        num_tables_full = len(full_schema["tables"])
        num_tables_predicted = len(predicted_schema["tables"])
        return num_tables_predicted / num_tables_full
    


    def column_proportion(self, predicted_schema: Schema, full_schema: Schema) -> float:
        full_schema_count = 0
        for table in full_schema["tables"]:
            full_schema_count += len(table["columns"])
        predicted_schema_count = 0
        for table in predicted_schema["tables"]:
            predicted_schema_count += len(table["columns"])
        return predicted_schema_count / full_schema_count
    


    def recall(self, correct: set, predicted: set):
        if len(correct) == 0:
            return 0
        return len(predicted.intersection(correct)) / len(correct)
    


    def precision(self, correct: set, predicted: set):
        if len(predicted) == 0:
            return 0
        return len(predicted.intersection(correct)) / len(predicted)
    


    def f1(self, correct: set, predicted: set):
        precision = self.precision(correct, predicted)
        recall = self.recall(correct, predicted)
        if precision + recall == 0:
            return 0
        return 2 * ((precision * recall) / (precision + recall))
    


    def missing_identifiers(self, correct: set, predicted: set):
        return correct.difference(predicted)
    


    def correct_identifiers(self, correct: set, predicted: set):
        return correct.intersection(predicted)
    


    def extra_identifiers(self, correct: set, predicted: set):
        return predicted.difference(correct)
