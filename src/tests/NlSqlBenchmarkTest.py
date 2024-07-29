from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark

def iter_test():
    bm = NlSqlBenchmark()
    bm.databases = ["one", "two", "three"]
    bm.active_database_questions = ["a", "b", "c"]
    results = []
    for i in bm:
        results.append((i["database"], i["question_number"]))
    return results == [("one", 1), ("one", 2), ("one", 3), 
                       ("two", 0), ("two", 1), ("two", 2), ("two", 3), 
                       ("three", 0), ("three", 1), ("three", 2), ("three", 3)]


def execute_query_test():
    bm = NlSqlBenchmark()
    bm.databases = ["one", "two", "three"]
    bm.active_database_questions = ["a", "b", "c"]
    result = bm.execute_query()
    return result == {"result_set": {}, "database": "one", "question": "a", "error_message": ""}
