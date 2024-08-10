from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.DinSqlSubsetter import DinSqlSubsetter
from SchemaSubsetEvaluator import SchemaSubsetEvaluator
import QueryProfiler

def main():
    bm_factory = NlSqlBenchmarkFactory()
    benchmark = bm_factory.build_benchmark("bird")
    subsetter = DinSqlSubsetter(benchmark=benchmark)
    evaluator = SchemaSubsetEvaluator()
    for question in benchmark:
        print(benchmark.active_question_no)
        subset = subsetter.get_schema_subset(
            question=question["question"],
            full_schema=question["schema"]
        )
        print(benchmark.active_question_no)
        scores = evaluator.evaluate_schema_subset(
            subset,
            question["question"],
            question["schema"],
            benchmark
        )
        print(benchmark.active_question_no)
        print(scores)
        break
        

if __name__ == "__main__":
    main()