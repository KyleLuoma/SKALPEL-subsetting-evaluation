from SchemaSubsetter.RslSqlSubsetter import RslSqlSubsetter
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark

def init_test():
    try:
        ss = RslSqlSubsetter(NlSqlBenchmark())
        return True
    except:
        return False
    

def preprocess_db_test():
    bm = NlSqlBenchmark()
    ss = RslSqlSubsetter(bm)
    times = ss.preprocess_databases()
    return "database1" in times.keys()