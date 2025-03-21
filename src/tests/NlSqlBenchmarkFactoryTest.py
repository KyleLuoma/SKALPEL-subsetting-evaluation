from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory


def init_benchmark_lookup_test():
    fact = NlSqlBenchmarkFactory()
    bm_lookup = fact._init_benchmark_lookup()
    benchmarks = set()
    for db in bm_lookup.keys():
        benchmarks.add(bm_lookup[db])
    return benchmarks == {"snails", "spider2", "bird"}


def lookup_benchmark_by_db_name_test():
    db_name = "NTSB"
    fact = NlSqlBenchmarkFactory()
    return fact.lookup_benchmark_by_db_name(db_name) == "snails"