"""
Class for evaluating a schema subset against the schema identifiers required for a NL-to-SQL question
"""

from SchemaSubsetter.PerfectSchemaSubsetter import PerfectSchemaSubsetter
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark

class SchemaSubsetEvaluator:

    def __init__(self):
        pass



    def evaluate_schema_subset(
            self,
            predicted_schema_subset: dict,
            question: str,
            full_schema: dict,
            benchmark: NlSqlBenchmark
            ) -> dict:
        
        subsetter = PerfectSchemaSubsetter(benchmark)
        
        correct_schema_subset = subsetter.get_schema_subset(
            question=question,
            full_schema=full_schema
        )

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

        return {
            "total_recall": self.recall(
                correct=subset_identifiers["correct"]["tables"].union(subset_identifiers["correct"]["columns"]),
                predicted=subset_identifiers["predicted"]["tables"].union(subset_identifiers["predicted"]["columns"])
                ),
            "total_precision": self.precision(
                correct=subset_identifiers["correct"]["tables"].union(subset_identifiers["correct"]["columns"]),
                predicted=subset_identifiers["predicted"]["tables"].union(subset_identifiers["predicted"]["columns"])
                ),
            "total_f1": self.f1(
                correct=subset_identifiers["correct"]["tables"].union(subset_identifiers["correct"]["columns"]),
                predicted=subset_identifiers["predicted"]["tables"].union(subset_identifiers["predicted"]["columns"])
                ),
            "table_recall": self.recall(
                correct=subset_identifiers["correct"]["tables"],
                predicted=subset_identifiers["predicted"]["tables"]
                ),
            "table_precision": self.precision(
                correct=subset_identifiers["correct"]["tables"],
                predicted=subset_identifiers["predicted"]["tables"]
                ),
            "table_f1": self.f1(
                correct=subset_identifiers["correct"]["tables"],
                predicted=subset_identifiers["predicted"]["tables"]
                ),
            "column_recall": self.recall(
                correct=subset_identifiers["correct"]["columns"],
                predicted=subset_identifiers["predicted"]["columns"]
                ),
            "column_precision": self.precision(
                correct=subset_identifiers["correct"]["columns"],
                predicted=subset_identifiers["predicted"]["columns"]
                ),
            "column_f1": self.f1(
                correct=subset_identifiers["correct"]["columns"],
                predicted=subset_identifiers["predicted"]["columns"]
                ),
            "correct_tables": self.correct_identifiers(
                correct=subset_identifiers["correct"]["tables"],
                predicted=subset_identifiers["predicted"]["tables"]
            ),
            "missing_tables": self.missing_identifiers(
                correct=subset_identifiers["correct"]["tables"],
                predicted=subset_identifiers["predicted"]["tables"]
            ),
            "extra_tables": self.extra_identifiers(
                correct=subset_identifiers["correct"]["tables"],
                predicted=subset_identifiers["predicted"]["tables"]
            ),
            "correct_columns": self.correct_identifiers(
                correct=subset_identifiers["correct"]["columns"],
                predicted=subset_identifiers["predicted"]["columns"]
            ),
            "missing_columns": self.missing_identifiers(
                correct=subset_identifiers["correct"]["columns"],
                predicted=subset_identifiers["predicted"]["columns"]
            ),
            "extra_columns": self.extra_identifiers(
                correct=subset_identifiers["correct"]["columns"],
                predicted=subset_identifiers["predicted"]["columns"]
            )
        }
    


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
