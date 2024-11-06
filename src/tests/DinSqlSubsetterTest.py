from SchemaSubsetter.DinSqlSubsetter import DinSqlSubsetter
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark


test_schema = {
        "tables": [
            {
                "name": "t1", "columns": [{"name": "c1", "type": "int"}],
                "primary_keys": ["c1"],
                "foreign_keys": [{"columns": ["c1"], "references": ("t2", ["c2"])}]                
                },
            {
                "name": "t2", "columns": [{"name": "c2", "type": "int"}],
                "primary_keys": ["c2"],
                "foreign_keys": []
                }
        ]
    }


din_prompt = """Table customers, columns = [*,CustomerID,Segment,Currency]
Table gasstations, columns = [*,GasStationID,ChainID,Country,Segment]
Table products, columns = [*,ProductID,Description]
Table transactions_1k, columns = [*,TransactionID,Date,Time,CustomerID,CardID,GasStationID,ProductID,Amount,Price]
Table yearmonth, columns = [*,CustomerID,Date,Consumption]
Foreign_keys =
[yearmonth.CustomerID = customers.CustomerID]
Q: "How many gas stations in CZE has Premium gas?"
A: Let's think step by step."""


def get_schema_subset_mock_openai_call_test():

    def mock_GPT4_generation(prompt: str):
        return """
In the question "How many gas stations in CZE has Premium gas?", we are asked:
"gas stations in CZE" so we need column = [gasstations.GasStationID,gasstations.Country]
"Premium gas" so we need column = [products.Description]
Based on the columns and tables, we need these Foreign_keys = [transactions_1k.GasStationID = gasstations.GasStationID, transactions_1k.ProductID = products.ProductID].
Based on the tables, columns, and Foreign_keys, The set of possible cell values are = ['CZE', 'Premium']. So the Schema_links are:
Schema_links: [gasstations.GasStationID,gasstations.Country,products.Description,transactions_1k.GasStationID = gasstations.GasStationID, transactions_1k.ProductID = products.ProductID,'CZE', 'Premium']
"""

    correct_subset = {
        'tables': [
            {
                'name': 'gasstations', 
                'columns': [
                    {'name': 'GasStationID', 'type': None}, 
                    {'name': 'Country', 'type': None}
                    ], 
                'primary_keys': [], 
                'foreign_keys': []
            }, {
                'name': 'products', 
                'columns': [
                    {'name': 'Description', 'type': None}, 
                    {'name': 'ProductID', 'type': None}
                    ], 
                'primary_keys': [], 
                'foreign_keys': []
            }, {
                'name': 'transactions_1k', 
                'columns': [
                    {'name': 'GasStationID', 'type': None}, 
                    {'name': 'ProductID', 'type': None}
                    ], 
                'primary_keys': [], 
                'foreign_keys': []
            }]}

    din_ss = DinSqlSubsetter(benchmark=BirdNlSqlBenchmark())

    din_ss.GPT4_generation = mock_GPT4_generation

    subset = din_ss.get_schema_subset(
        din_ss.benchmark.get_active_question()["question"], 
        din_ss.benchmark.get_active_schema()
        )
    return subset == correct_subset


def transform_schema_to_din_sql_format_test():
    din_ss = DinSqlSubsetter(benchmark=BirdNlSqlBenchmark())
    correct_schema = "Table t1, columns = [*,c1]\nTable t2, columns = [*,c2]\n"
    output_schema = din_ss.transform_schema_to_dinsql_format(test_schema)
    return output_schema == correct_schema


def transform_dependencies_to_dinsql_format_test():
    din_ss = DinSqlSubsetter(benchmark=BirdNlSqlBenchmark())
    output_dependencies = din_ss.transform_dependencies_to_dinsql_format(test_schema)
    return output_dependencies == "[t1.c1 = t2.c2]"


def schema_linking_prompt_maker_test():
    din_ss = DinSqlSubsetter(benchmark=BirdNlSqlBenchmark())
    prompt = din_ss.schema_linking_prompt_maker(
        din_ss.benchmark.get_active_question()["question"], 
        din_ss.benchmark.get_active_schema()
    )
    return (
        "How many gas stations in CZE has Premium gas?" in prompt
        and "[yearmonth.CustomerID = customers.CustomerID]" in prompt
        and "Table transactions_1k, columns = [*,TransactionID,Date,Time,CustomerID,CardID,GasStationID,ProductID,Amount,Price]" in prompt
        )