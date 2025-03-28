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
    result = chess.get_schema_subset(bm.get_active_question())
    return type(result.schema_subset) == Schema


def get_token_counts_from_log_test():
    token_counts = ChessSubsetter.get_token_counts_from_log("NTSB", 0)
    stage_sums = {}
    output_sums = {}
    for item in token_counts:
        print(item)
        if item["stage"] not in stage_sums.keys():
            stage_sums[item["stage"]] = item["input_token_count"]
            output_sums[item["stage"]] = item["output_token_count"]
        else:
            stage_sums[item["stage"]] += item["input_token_count"]
            output_sums[item["stage"]] += item["output_token_count"]
    print("Stage sums input:", stage_sums)
    print("Stage sums output:", output_sums)
    return True