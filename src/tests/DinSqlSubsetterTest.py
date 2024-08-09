from SchemaSubsetter.DinSqlSubsetter import DinSqlSubsetter
from NlSqlBenchmark.BirdNlSqlBenchmark import BirdNlSqlBenchmark


def get_schema_subset_test():
    din_ss = DinSqlSubsetter(benchmark=BirdNlSqlBenchmark())
    subset = din_ss.get_schema_subset("question", din_ss.benchmark.get_active_schema())
    print(subset)
    return subset == {}


def transform_schema_to_din_sql_format_test():
    din_ss = DinSqlSubsetter(benchmark=BirdNlSqlBenchmark())
    input_schema = {
        "tables": [
            {"name": "t1", "columns": [{"name": "c1", "type": "int"}]},
            {"name": "t2", "columns": [{"name": "c2", "type": "int"}]}
        ]
    }
    correct_schema = "Table t1, columns = [*,c1]\nTable t2, columns = [*,c2]\n"
    output_schema = din_ss.transform_schema_to_dinsql_format(input_schema)
    return output_schema == correct_schema