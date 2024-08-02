from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
import SchemaSubsetter.SchemaSubsetter as SchemaSubsetter
import SubsetEvaluator
import QueryProfiler

def main():
    bm_factory = NlSqlBenchmarkFactory()
    benchmark = bm_factory.build_benchmark("bird")

    benchmark.set_active_schema("california_schools")

    res = benchmark.get_active_schema(database="california_schools")

    qp = QueryProfiler.QueryProfiler()
    qp_res = qp.profile_query("SELECT A FROM ONE")
    print(qp_res)
        

if __name__ == "__main__":
    main()