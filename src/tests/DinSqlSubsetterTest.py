from SchemaSubsetter.DinSqlSubsetter import DinSqlSubsetter
from NlSqlBenchmark.BirdNlSqlBenchmark import BirdNlSqlBenchmark


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


def get_schema_subset_test():
    din_ss = DinSqlSubsetter(benchmark=BirdNlSqlBenchmark())
    subset = din_ss.get_schema_subset(
        din_ss.benchmark.get_active_question()["question"], 
        din_ss.benchmark.get_active_schema()
        )
    return subset == {}


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