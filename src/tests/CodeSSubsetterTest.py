from SchemaSubsetter.CodeSSubsetter import CodeSSubsetter
from NlSqlBenchmark.BirdNlSqlBenchmark import BirdNlSqlBenchmark

def adapt_benchmark_schema_test():
    benchmark = BirdNlSqlBenchmark()
    subsetter = CodeSSubsetter(benchmark)
    adapted_schema = subsetter.adapt_benchmark_schema(benchmark.get_active_schema())
    print(adapted_schema)
    return False


def filter_schema_test():
    benchmark = BirdNlSqlBenchmark()
    subsetter = CodeSSubsetter(benchmark)
    filtered = subsetter.sic.predict_one(
        subsetter.adapt_benchmark_schema(benchmark.get_active_schema())[0]
    )
    print(filtered)
    return False
