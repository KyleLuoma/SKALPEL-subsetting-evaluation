from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark
from NlSqlBenchmark.QueryResult import QueryResult
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion

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


def iter_reset_test():
    bird = BirdNlSqlBenchmark()
    for q in bird:
        pass
    return bird.active_database == 0 and bird.active_question_no == 0


def execute_query_valid_query_test():
    bird = BirdNlSqlBenchmark()
    query = "SELECT CharterNum, AvgScrWrite, RANK() OVER (ORDER BY AvgScrWrite DESC) AS WritingScoreRank FROM schools AS T1  INNER JOIN satscores AS T2 ON T1.CDSCode = T2.cds WHERE T2.AvgScrWrite > 499 AND CharterNum is not null LIMIT 3"
    correct_result = QueryResult(
        result_set={
            "CharterNum": ["0210", "0890", "0290"], 
            "AvgScrWrite": [630, 593, 582],
            "WritingScoreRank": [1, 2, 3]
            }, 
        database="california_schools", 
        question=0, 
        error_message=None
    )
    res = bird.execute_query(query=query, database="california_schools")
    return res == correct_result



def execute_query_syntax_error_test():
    bird = BirdNlSqlBenchmark()
    query = "SELECT ChartnerNum FORM schools"
    correct_result = QueryResult(
        result_set=None, 
        database=None, 
        question=None, 
        error_message='near "schools": syntax error'
    )
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
    return pass_test
    


def get_active_question_test():
    bird = BirdNlSqlBenchmark()
    result = bird.get_active_question()
    return result == BenchmarkQuestion(
        question="How many gas stations in CZE has Premium gas?",
        query="SELECT COUNT(GasStationID) FROM gasstations WHERE Country = 'CZE' AND Segment = 'Premium'",
        query_dialect=bird.sql_dialect,
        question_number=0,
        schema=Schema(
            database="debit_card_specializing", 
            tables=[
                SchemaTable(
                    name='customers', 
                    columns=[
                        TableColumn(name='CustomerID', data_type='integer', description='identification of the customer'), 
                        TableColumn(name='Segment', data_type='text', description='client segment'), 
                        TableColumn(name='Currency', data_type='text', description='Currency')
                    ], 
                    primary_keys=['CustomerID'], 
                    foreign_keys=[]
                    ),
                SchemaTable(
                    name='gasstations', 
                    columns=[
                        TableColumn(name='GasStationID', data_type='integer', description='Gas Station ID'), 
                        TableColumn(name='ChainID', data_type='integer', description='Chain ID'), 
                        TableColumn(name='Country', data_type='text', description=''), 
                        TableColumn(name='Segment', data_type='text', description='chain segment')
                        ], 
                    primary_keys=['GasStationID'], 
                    foreign_keys=[]
                    ),
                SchemaTable(
                    name='products', 
                    columns=[
                        TableColumn(name='ProductID', data_type='integer', description='Product ID'), 
                        TableColumn(name='Description', data_type='text', description='Description')
                        ], 
                    primary_keys=['ProductID'], 
                    foreign_keys=[]
                    ),
                SchemaTable(
                    name='transactions_1k', 
                    columns=[
                        TableColumn(name='TransactionID', data_type='integer', description='Transaction ID'), 
                        TableColumn(name='Date', data_type='date', description='Date'), 
                        TableColumn(name='Time', data_type='text', description='Time'), 
                        TableColumn(name='CustomerID', data_type='integer', description='Customer ID'), 
                        TableColumn(name='CardID', data_type='integer', description='Card ID'), 
                        TableColumn(name='GasStationID', data_type='integer', description='Gas Station ID'), 
                        TableColumn(name='ProductID', data_type='integer', description='Product ID'), 
                        TableColumn(name='Amount', data_type='integer', description='Amount'), 
                        TableColumn(name='Price', data_type='real', description='Price', value_description='"commonsense evidence: total price = Amount x Price"')
                        ], 
                    primary_keys=['TransactionID'], 
                    foreign_keys=[]
                    ),
                SchemaTable(
                    name='yearmonth', 
                    columns=[
                        TableColumn(name='CustomerID', data_type='integer', description='Customer ID'), 
                        TableColumn(name='Date', data_type='text', description='Date'), 
                        TableColumn(name='Consumption', data_type='real', description='consumption')
                        ], 
                    primary_keys=['CustomerID', 'Date'], 
                    foreign_keys=[
                        ForeignKey(columns=['CustomerID'], references=('customers', ['CustomerID']))
                        ]
                    )
                ]
            )
        )


def get_sample_values_test():
    bird = BirdNlSqlBenchmark()
    sample_values = bird.get_sample_values(
        database="debit_card_specializing",
        table_name="products",
        column_name="Description"
    )
    return sample_values == ['Rucní zadání', 'Nafta']


def get_unique_values_test():
    bird = BirdNlSqlBenchmark()
    unique_values = bird.get_unique_values(
        table_name="products",
        column_name="Description",
        database="debit_card_specializing"
    )
    return len(unique_values) == 529