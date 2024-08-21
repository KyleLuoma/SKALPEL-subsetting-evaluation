from SchemaSubsetEvaluator import SchemaSubsetEvaluator
from NlSqlBenchmark.BirdNlSqlBenchmark import BirdNlSqlBenchmark


def evaluate_schema_subset_test():
    sse = SchemaSubsetEvaluator()
    benchmark = BirdNlSqlBenchmark()

    predicted_subset = {
        "tables": [
            {
                "name": "gasstations", 
                "columns": [
                    {"name": "GasStationID", "type": "integer"}, 
                    {"name": "Country", "type": "text"}, 
                    {"name": "FalsePositive1", "type": "text"},
                    {"name": "FalsePositive2", "type": "text"}
                    ]
            },
            {
                "name": "FalseTable1",
                "columns": [
                    {"name": "FalsePositive3", "type": "text"}
                ]
            }
        ]}

    correct_scores = {
        "total_recall": 0.75, 
        "total_precision": 0.42857142857142855, 
        "total_f1": 0.5454545454545454, 
        "table_recall": 1.0, 
        "table_precision": 0.5, 
        "table_f1": 0.6666666666666666, 
        "column_recall": 0.6666666666666666, 
        "column_precision": 0.4, 
        "column_f1": 0.5,
        "correct_tables": {"gasstations"}, 
        "missing_tables": set(), 
        "extra_tables": {"FalseTable1"}, 
        "correct_columns": {"gasstations.GasStationID", "gasstations.Country"}, 
        "missing_columns": {"gasstations.Segment"}, 
        "extra_columns": {
            "FalseTable1.FalsePositive3", 
            "gasstations.FalsePositive1", 
            "gasstations.FalsePositive2"
            }
        }

    question = benchmark.get_active_question()
    scores = sse.evaluate_schema_subset(
        predicted_subset, 
        question["question"],
        full_schema=question["schema"],
        benchmark=benchmark
        )
    return scores == correct_scores



def recall_test():
    sse = SchemaSubsetEvaluator()
    correct = {"A", "B"}
    predicted = {"A", "C"}
    return sse.recall(correct, predicted) == 0.5



def precision_test():
    sse = SchemaSubsetEvaluator()
    correct = {"A", "B"}
    predicted = {"A", "C", "D", "E"}
    return sse.precision(correct, predicted) == 0.25



def f1_test():
    sse = SchemaSubsetEvaluator()
    correct = {"A", "B"}
    predicted = {"A", "C", "D", "E"}
    return sse.f1(correct, predicted) == 2 * ((0.25 * 0.5) / (0.25 + 0.5))


