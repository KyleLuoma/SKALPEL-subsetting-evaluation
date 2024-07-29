from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
import SchemaSubsetter
import SubsetEvaluator

def main():
    bm_factory = NlSqlBenchmarkFactory()
    benchmark = bm_factory.build_benchmark("bird")
    print(benchmark.get_active_question())
    for q in benchmark:
        pass
    print(benchmark.get_active_question())
        

if __name__ == "__main__":
    main()