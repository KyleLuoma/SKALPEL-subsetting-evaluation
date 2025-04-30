from SchemaSubsetter.RslSqlSubsetter import RslSqlSubsetter
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import Schema

# def init_test():
#     try:
#         ss = RslSqlSubsetter(NlSqlBenchmark())
#         return True
#     except:
#         return False
    

# def preprocess_db_test():
#     bm = NlSqlBenchmark()
#     ss = RslSqlSubsetter(bm)
#     times = ss.preprocess_databases()
#     return "database1" in times.keys()


def get_schema_subset_test():
    bm = BirdNlSqlBenchmark()
    ss = RslSqlSubsetter(bm)
    question = bm.get_active_question()
    subset_result = ss.get_schema_subset(question)
    print(subset_result.schema_subset)
    return type(subset_result.schema_subset) == Schema