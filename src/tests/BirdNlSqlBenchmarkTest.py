from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark

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
        found_databases.add(q["schema"]["database"])
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
        "question_number": 0,
        'schema': {
            'database': "debit_card_specializing",
            'tables': [
                {
                    'name': 'customers', 
                    'columns': [
                        {'name': 'CustomerID', 'type': 'integer'}, 
                        {'name': 'Segment', 'type': 'text'}, 
                        {'name': 'Currency', 'type': 'text'}
                        ], 
                    'primary_keys': [['CustomerID']], 
                    'foreign_keys': []
                    }, 
                {
                    'name': 'gasstations', 
                    'columns': [
                        {'name': 'GasStationID', 'type': 'integer'}, 
                        {'name': 'ChainID', 'type': 'integer'}, 
                        {'name': 'Country', 'type': 'text'}, 
                        {'name': 'Segment', 'type': 'text'}
                        ], 
                    'primary_keys': [['GasStationID']], 
                    'foreign_keys': []
                    }, 
                {
                    'name': 'products', 
                    'columns': [
                        {'name': 'ProductID', 'type': 'integer'}, 
                        {'name': 'Description', 'type': 'text'}
                        ], 
                    'primary_keys': [['ProductID']], 
                    'foreign_keys': []
                    }, 
                {
                    'name': 'transactions_1k', 
                    'columns': [
                        {'name': 'TransactionID', 'type': 'integer'}, 
                        {'name': 'Date', 'type': 'date'}, 
                        {'name': 'Time', 'type': 'text'}, 
                        {'name': 'CustomerID', 'type': 'integer'}, 
                        {'name': 'CardID', 'type': 'integer'}, 
                        {'name': 'GasStationID', 'type': 'integer'}, 
                        {'name': 'ProductID', 'type': 'integer'}, 
                        {'name': 'Amount', 'type': 'integer'}, 
                        {'name': 'Price', 'type': 'real'}
                        ], 
                    'primary_keys': [['TransactionID']], 
                    'foreign_keys': []
                    }, 
                {
                    'name': 'yearmonth', 
                    'columns': [
                        {'name': 'CustomerID', 'type': 'integer'}, 
                        {'name': 'Date', 'type': 'text'}, 
                        {'name': 'Consumption', 'type': 'real'}
                        ], 
                    'primary_keys': [['CustomerID', 'Date']], 
                    'foreign_keys': [{'columns': ['CustomerID'], 'references': ('customers', ['CustomerID'])}]
                    }
                ]
            }
        }



def get_sample_values_test():
    bird = BirdNlSqlBenchmark()
    sample_values = bird.get_sample_values(
        database="debit_card_specializing",
        table_name="products",
        column_name="Description"
    )
    return sample_values == ['Rucní zadání', 'Nafta']