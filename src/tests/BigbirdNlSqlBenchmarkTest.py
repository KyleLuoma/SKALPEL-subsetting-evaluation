from NlSqlBenchmark.bigbird.BigBirdNlSqlBenchmark import BigBirdNlSqlBenchmark
from NlSqlBenchmark.QueryResult import QueryResult
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion

bigbird_databases = {
    "debit_card_specializing_big", 
    "financial_big", 
    "formula_1_big", 
    "california_schools_big",
    "card_games_big",
    "european_football_2_big",
    "thrombosis_prediction_big",
    "toxicology_big",
    "student_club_big",
    "superhero_big",
    "codebase_community_big"
    }

def iter_test():
    bird = BigBirdNlSqlBenchmark()
    found_databases = set()
    questions = []
    for q in bird:
        found_databases.add(q["schema"]["database"])
        questions.append(1)
    return found_databases == bigbird_databases and len(questions) == 1534


def iter_reset_test():
    bbird = BigBirdNlSqlBenchmark()
    for q in bbird:
        pass
    return bbird.active_database == 0 and bbird.active_question_no == 0


def execute_query_valid_query_test():
    bbird = BigBirdNlSqlBenchmark()
    query = "SELECT CharterNum, AvgScrWrite, RANK() OVER (ORDER BY AvgScrWrite DESC) AS WritingScoreRank FROM schools AS T1  INNER JOIN satscores AS T2 ON T1.CDSCode = T2.cds WHERE T2.AvgScrWrite > 499 AND CharterNum is not null LIMIT 3"
    correct_result = QueryResult(
        result_set={
            "CharterNum": ["0210", "0890", "0290"], 
            "AvgScrWrite": [630, 593, 582],
            "WritingScoreRank": [1, 2, 3]
            }, 
        database="california_schools_big", 
        question=0, 
        error_message=None
    )
    res = bbird.execute_query(query=query, database="california_schools_big")
    return res == correct_result


def execute_query_syntax_error_test():
    bbird = BigBirdNlSqlBenchmark()
    query = "SELECT ChartnerNum FORM schools"
    correct_result = QueryResult(
        result_set=None, 
        database=None, 
        question=None, 
        error_message='near "schools": syntax error'
    )
    res = bbird.execute_query(query=query, database="california_schools_big")
    return res == correct_result


def set_and_get_active_schema_test():
    bird = BigBirdNlSqlBenchmark()
    bird.set_active_schema("california_schools_big")
    s_data = bird.get_active_schema()
    # for t in s_data["tables"]:
    #     for k in t:
    #         print(k, ":", t[k])
    pass_test = True
    pass_test = s_data.database == "california_schools_big"
    return pass_test
    

def get_sample_values_test():
    bird = BigBirdNlSqlBenchmark()
    sample_values = bird.get_sample_values(
        database="debit_card_specializing_big",
        table_name="products",
        column_name="Description"
    )
    return sample_values == ['Rucní zadání', 'Nafta']


def get_unique_values_test():
    bird = BigBirdNlSqlBenchmark()
    unique_values = bird.get_unique_values(
        table_name="products",
        column_name="Description",
        database="debit_card_specializing_big"
    )
    return len(unique_values) == 529



