from SchemaSubsetter.ChessSubsetter import ChessSubsetter
from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
from NlSqlBenchmark.SchemaObjects import Schema

def init_test():
    bm_fact = NlSqlBenchmarkFactory()
    bm = bm_fact.build_benchmark("bird")
    try:
        chess = ChessSubsetter(do_preprocessing=False, benchmark=bm)
    except Exception as e:
        print(e)
        return False
    return True


def get_schema_subset_test():
    bm_fact = NlSqlBenchmarkFactory()
    bm = bm_fact.build_benchmark("bird")
    chess = ChessSubsetter(do_preprocessing=False, benchmark=bm)
    subset = chess.get_schema_subset(bm.get_active_question())
    return type(subset) == Schema