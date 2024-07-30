from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
import SchemaSubsetter
import SubsetEvaluator

def main():
    bm_factory = NlSqlBenchmarkFactory()
    benchmark = bm_factory.build_benchmark("bird")

    benchmark.set_active_schema("california_schools")

    res = benchmark.get_active_schema(database="california_schools")
    print(res)
        

if __name__ == "__main__":
    main()