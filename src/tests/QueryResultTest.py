from NlSqlBenchmark.QueryResult import QueryResult


def init_and_get_test():
    result = QueryResult(
        result_set={"A": "B"},
        database="C",
        question=1,
        error_message="D"
    )
    return result["database"] == "C"

