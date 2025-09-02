from SchemaSubsetter.DinSqlSubsetter import DinSqlSubsetter
from SchemaSubsetter.SchemaSubsetterResult import SchemaSubsetterResult
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark
from NlSqlBenchmark.spider2.Spider2NlSqlBenchmark import Spider2NlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion



test_schema = Schema(
        database="test_schema",
        tables=[
            SchemaTable(
                name="t1",
                columns=[TableColumn(name="c1", data_type="int")],
                primary_keys=["c1"],
                foreign_keys=[
                    ForeignKey(
                        columns=["c1"],
                        references=("t2", ["c2"])
                    )
                ]
            ),
            SchemaTable(
                name="t2",
                columns=[TableColumn(name="c2", data_type="int")],
                primary_keys=["c2"],
                foreign_keys=[]
            )
        ]
    )


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

    def mock_GPT4_generation(prompt: str, model: str = "gpt-4.1"):
        return ("""
In the question "How many gas stations in CZE has Premium gas?", we are asked:
"gas stations in CZE" so we need column = [gasstations.GasStationID,gasstations.Country]
"Premium gas" so we need column = [products.Description]
Based on the columns and tables, we need these Foreign_keys = [transactions_1k.GasStationID = gasstations.GasStationID, transactions_1k.ProductID = products.ProductID].
Based on the tables, columns, and Foreign_keys, The set of possible cell values are = ['CZE', 'Premium']. So the Schema_links are:
Schema_links: [gasstations.GasStationID,gasstations.Country,products.Description,transactions_1k.GasStationID = gasstations.GasStationID, transactions_1k.ProductID = products.ProductID,'CZE', 'Premium']
""", 100)

    correct_subset = Schema(
        database="debit_card_specializing",
        tables=[
            SchemaTable(
                name="gasstations",
                columns=[
                    TableColumn(name="GasStationID", data_type=None),
                    TableColumn(name="Country", data_type=None)
                ],
                primary_keys=[],
                foreign_keys=[]
            ),
            SchemaTable(
                name="products",
                columns=[
                    TableColumn(name="Description", data_type=None),
                    TableColumn(name="ProductID", data_type=None)
                ],
                primary_keys=[],
                foreign_keys=[]
            ),
            SchemaTable(
                name="transactions_1k",
                columns=[
                    TableColumn(name="GasStationID", data_type=None),
                    TableColumn(name="ProductID", data_type=None)
                ],
                primary_keys=[],
                foreign_keys=[]
            )
        ]
    )
    din_ss = DinSqlSubsetter(benchmark=BirdNlSqlBenchmark())

    din_ss.GPT4_generation = mock_GPT4_generation

    subset_result = din_ss.get_schema_subset(
        benchmark_question=din_ss.benchmark.get_active_question()
        )
    return subset_result.schema_subset == correct_subset


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


def get_schema_subset_test():
    din_ss = DinSqlSubsetter(benchmark=BirdNlSqlBenchmark())
    bm = BirdNlSqlBenchmark()
    question = bm.get_active_question()
    ss_result = din_ss.get_schema_subset(benchmark_question=question)
    return type(ss_result) == SchemaSubsetterResult


# def get_spider2_subset_test():
#     din_ss = DinSqlSubsetter(benchmark=Spider2NlSqlBenchmark())
#     din_ss.get_schema_subset(benchmark_question=din_ss.benchmark.get_active_question())
#     return False