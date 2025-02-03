from SubsetEvaluator.SchemaSubsetEvaluator import SchemaSubsetEvaluator
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from SubsetEvaluator.SubsetEvaluation import SubsetEvaluation


def evaluate_schema_subset_no_cache_test():
    sse = SchemaSubsetEvaluator(use_result_cache=False)
    benchmark = BirdNlSqlBenchmark()
    
    predicted_subset = Schema(
        database="debit_card_specializing",
        tables=[
            SchemaTable(
                name="gasstations",
                columns=[
                    TableColumn(name="GasStationID", data_type="integer"),
                    TableColumn(name="Country", data_type="text"),
                    TableColumn(name="FalsePositive1", data_type="text"),
                    TableColumn(name="FalsePositive2", data_type="text")
                ],
                primary_keys=[],
                foreign_keys=[]
            ),
            SchemaTable(
                name="FalseTable1",
                columns=[
                    TableColumn(name="FalsePositive3", data_type="text")
                ],
                primary_keys=[],
                foreign_keys=[]
            )
        ]
    )
    
    correct_scores = SubsetEvaluation(
        total_recall=0.75,
        total_precision=0.42857142857142855,
        total_f1=0.5454545454545454,
        table_recall=1.0,
        table_precision=0.5,
        table_f1=0.6666666666666666,
        column_recall=0.6666666666666666,
        column_precision=0.4,
        column_f1=0.5,
        correct_tables={"gasstations"},
        missing_tables=set(),
        extra_tables={"FalseTable1"},
        correct_columns={"gasstations.GasStationID", "gasstations.Country"},
        missing_columns={"gasstations.Segment"},
        extra_columns={
            "FalseTable1.FalsePositive3",
            "gasstations.FalsePositive1",
            "gasstations.FalsePositive2"
        },
        subset_table_proportion=0.4,
        subset_column_proportion=0.23809523809523808
    )

    question = benchmark.get_active_question()
    scores = sse.evaluate_schema_subset(
        predicted_subset, 
        question
        )
    return scores == correct_scores


def evaluate_schema_subset_with_cache_test():
    sse = SchemaSubsetEvaluator(use_result_cache=True)
    benchmark = BirdNlSqlBenchmark()
    
    predicted_subset = Schema(
        database="debit_card_specializing",
        tables=[
            SchemaTable(
                name="gasstations",
                columns=[
                    TableColumn(name="GasStationID", data_type="integer"),
                    TableColumn(name="Country", data_type="text"),
                    TableColumn(name="FalsePositive1", data_type="text"),
                    TableColumn(name="FalsePositive2", data_type="text")
                ],
                primary_keys=[],
                foreign_keys=[]
            ),
            SchemaTable(
                name="FalseTable1",
                columns=[
                    TableColumn(name="FalsePositive3", data_type="text")
                ],
                primary_keys=[],
                foreign_keys=[]
            )
        ]
    )
    
    correct_scores = SubsetEvaluation(
        total_recall=0.75,
        total_precision=0.42857142857142855,
        total_f1=0.5454545454545454,
        table_recall=1.0,
        table_precision=0.5,
        table_f1=0.6666666666666666,
        column_recall=0.6666666666666666,
        column_precision=0.4,
        column_f1=0.5,
        correct_tables={"gasstations"},
        missing_tables=set(),
        extra_tables={"FalseTable1"},
        correct_columns={"gasstations.GasStationID", "gasstations.Country"},
        missing_columns={"gasstations.Segment"},
        extra_columns={
            "FalseTable1.FalsePositive3",
            "gasstations.FalsePositive1",
            "gasstations.FalsePositive2"
        },
        subset_table_proportion=0.4,
        subset_column_proportion=0.23809523809523808
    )

    question = benchmark.get_active_question()
    scores = sse.evaluate_schema_subset(
        predicted_subset, 
        question
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


