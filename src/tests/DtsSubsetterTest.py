from SchemaSubsetter.DtsSubsetter import DtsSubsetter
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark
from SchemaSubsetter.SchemaSubsetterResult import SchemaSubsetterResult

def init_test():
    bm = NlSqlBenchmark()
    try:
        dts = DtsSubsetter(benchmark=bm)
        return True
    except:
        return False
    

def get_schema_subset_test():
    bm = BirdNlSqlBenchmark()
    dts = DtsSubsetter(benchmark=bm)
    result = dts.get_schema_subset(benchmark_question=bm.get_active_question())
    print(result.schema_subset)
    return type(result)==SchemaSubsetterResult