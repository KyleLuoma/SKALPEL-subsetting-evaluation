from SchemaSubsetter.ChessSubsetter import ChessSubsetter
from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory

def init_test():
    bm_fact = NlSqlBenchmarkFactory()
    bm = bm_fact.build_benchmark("bird")
    try:
        chess = ChessSubsetter(do_preprocessing=False, benchmark=bm)
    except Exception as e:
        print(e)
        return False
    return True