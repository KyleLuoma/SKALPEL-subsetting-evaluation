from NlSqlBenchmark.QueryResult import QueryResult


def init_and_get_test():
    result = QueryResult(
        result_set={"A": "B"},
        database="C",
        question=1,
        error_message="D"
    )
    return result["database"] == "C"



def eq_true_test():
    result1 = QueryResult(
        result_set={"A": "B"},
        database="C",
        question=1,
        error_message="D"
    )
    result2 = result1
    return result1 == result2


def eq_different_types_test():
    result = QueryResult(
        result_set={"A": "B"},
        database="C",
        question=1,
        error_message="D"
    )
    return result != "This is a string"


def raise_key_error_test():
    result = QueryResult(
        result_set={"A": "B"},
        database="C",
        question=1,
        error_message="D"
    )
    try:
        result["not_exists"]
    except KeyError as e:
        return True
    return False