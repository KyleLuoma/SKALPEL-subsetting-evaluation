from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
import SchemaSubsetter
import SubsetEvaluator

def main():
    print("Hello, world!")
    bm_factory =NlSqlBenchmarkFactory()
    benchmark = bm_factory.build_benchmark("bird")
    benchmark.get_active_schema()
    for question in benchmark:
        print(question)

if __name__ == "__main__":
    main()