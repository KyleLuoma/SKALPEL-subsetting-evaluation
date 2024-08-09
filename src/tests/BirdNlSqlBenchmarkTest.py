from NlSqlBenchmark.BirdNlSqlBenchmark import BirdNlSqlBenchmark

bird_databases = {
    "debit_card_specializing", 
    "financial", 
    "formula_1", 
    "california_schools",
    "card_games",
    "european_football_2",
    "thrombosis_prediction",
    "toxicology",
    "student_club",
    "superhero",
    "codebase_community"
    }

def iter_test():
    bird = BirdNlSqlBenchmark()
    found_databases = set()
    questions = []
    for q in bird:
        found_databases.add(q["database"])
        questions.append(1)
    return found_databases == bird_databases and len(questions) == 1534



def execute_query_valid_query_test():
    bird = BirdNlSqlBenchmark()
    query = "SELECT CharterNum, AvgScrWrite, RANK() OVER (ORDER BY AvgScrWrite DESC) AS WritingScoreRank FROM schools AS T1  INNER JOIN satscores AS T2 ON T1.CDSCode = T2.cds WHERE T2.AvgScrWrite > 499 AND CharterNum is not null LIMIT 3"
    correct_result = {
        "result_set": {
            "CharterNum": ["0210", "0890", "0290"], 
            "AvgScrWrite": [630, 593, 582],
            "WritingScoreRank": [1, 2, 3]
            }, 
        "database": "california_schools", 
        "question": 0, 
        "error_message": ""
        }
    res = bird.execute_query(query=query, database="california_schools")
    return res == correct_result



def execute_query_syntax_error_test():
    bird = BirdNlSqlBenchmark()
    query = "SELECT ChartnerNum FORM schools"
    correct_result = {
        "result_set": None, 
        "database": None, 
        "question": None, 
        "error_message": 'near "schools": syntax error'
        }
    res = bird.execute_query(query=query, database="california_schools")
    return res == correct_result



def set_and_get_active_schema_test():
    bird = BirdNlSqlBenchmark()
    bird.set_active_schema("california_schools")
    s_data = bird.get_active_schema()
    # for t in s_data["tables"]:
    #     for k in t:
    #         print(k, ":", t[k])
    pass_test = True
    pass_test = (len(s_data["tables"]) == 3)
    for t in s_data["tables"]:
        pass_test = (set(t.keys()) == {"name", "columns", "primary_keys", "foreign_keys"})
    return pass_test
    


def get_active_question_test():
    bird = BirdNlSqlBenchmark()
    result = bird.get_active_question()
    return result == {
        "question": "How many gas stations in CZE has Premium gas?", 
        "query": "SELECT COUNT(GasStationID) FROM gasstations WHERE Country = 'CZE' AND Segment = 'Premium'", 
        "database": "debit_card_specializing", 
        "question_number": 0
        }