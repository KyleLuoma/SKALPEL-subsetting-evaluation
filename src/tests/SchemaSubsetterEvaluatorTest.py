from SchemaSubsetEvaluator import SchemaSubsetEvaluator
from NlSqlBenchmark.BirdNlSqlBenchmark import BirdNlSqlBenchmark


def evaluate_schema_subset_test():
    sse = SchemaSubsetEvaluator()
    benchmark = BirdNlSqlBenchmark()

    predicted_subset = {
        'tables': [
            {
                'name': 'gasstations', 
                'columns': [
                    {'name': 'GasStationID', 'type': 'integer'}, 
                    {'name': 'Country', 'type': 'text'}, 
                    {'name': 'Segment', 'type': 'text'}
                    ]}]}

    scores = sse.evaluate_schema_subset(predicted_subset, benchmark)
    print(scores)
    return False



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