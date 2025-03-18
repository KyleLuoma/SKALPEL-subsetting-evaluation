from SchemaSubsetter.ChessSubsetter import ChessSubsetter
from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory

def init_test():
    bm_fact = NlSqlBenchmarkFactory()
    bm = bm_fact.build_benchmark("snails")
    chess = ChessSubsetter(do_preprocessing=True, benchmark=bm)