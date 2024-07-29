from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
import SchemaSubsetter
import SubsetEvaluator

def main():
    print("Hello, world!")
    bm_factory =NlSqlBenchmarkFactory()
    benchmark = bm_factory.build_benchmark("snails")

if __name__ == "__main__":
    main()