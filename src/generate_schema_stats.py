from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
import os
current_directory = os.getcwd()
os.chdir(os.path.dirname(current_directory))
print(current_directory)
bm_fact = NlSqlBenchmarkFactory()
for bm_name in bm_fact.benchmark_register:
    print(bm_name)
    if bm_name in ["abstract"]:
        continue
    bm = bm_fact.build_benchmark(bm_name)
    bm.save_stats_to_disk("./subsetting_results/benchmark_schema_stats")