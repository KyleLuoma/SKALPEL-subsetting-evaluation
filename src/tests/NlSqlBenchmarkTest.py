from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark

def iter_test():
    bm = NlSqlBenchmark()
    bm.databases = ["one", "two", "three"]
    bm.active_database_questions = ["a", "b", "c"]
    bm.active_database_queries = [
        "SELECT A FROM ONE",
        "SELECT B FROM TWO",
        "SELECT C FROM THREE"
    ]
    results = []
    for i in bm:
        results.append((i["database"], i["question_number"]))
    return results == [("one", 0), ("one", 1), ("one", 2), 
                       ("two", 0), ("two", 1), ("two", 2), 
                       ("three", 0), ("three", 1), ("three", 2)]


def execute_query_test():
    bm = NlSqlBenchmark()
    bm.databases = ["one", "two", "three"]
    bm.active_database_questions = ["a", "b", "c"]
    result = bm.execute_query(query="")
    return result == {"result_set": {}, "database": "one", "question": "a", "error_message": ""}


def get_active_question_test():
    bm = NlSqlBenchmark()
    bm.databases = ["one", "two", "three"]
    bm.active_database_questions = ["a", "b", "c"]
    bm.active_database_queries = [
        "SELECT A FROM ONE",
        "SELECT B FROM TWO",
        "SELECT C FROM THREE"
    ]
    result = bm.get_active_question()
    return result == {
        "question": "a", 
        "query": "SELECT A FROM ONE", 
        "database": "one", "question_number": 0
        }
