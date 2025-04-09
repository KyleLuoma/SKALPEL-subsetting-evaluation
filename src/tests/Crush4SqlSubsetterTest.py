
from SchemaSubsetter.Crush4SqlSubsetter import Crush4SqlSubsetter
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema, SchemaTable, TableColumn
)
import os

def init_test():
    benchmark = NlSqlBenchmark()
    try:
        crush = Crush4SqlSubsetter(benchmark)
        return True
    except:
        return False


def flatten_schema_test():
    benchmark = NlSqlBenchmark()
    crush = Crush4SqlSubsetter(benchmark=benchmark)
    crush.preprocess_databases()
    flattened = crush._flatten_schema(crush.benchmark.get_active_schema())
    print(flattened)
    return flattened == ['database1 table1 1000 column1']


def register_table_test():
    crush = Crush4SqlSubsetter(benchmark=NlSqlBenchmark())
    current_code = crush.next_code
    crush._register_table("database.table")
    return (
        crush.next_code == current_code + 1
        and crush.code_table_lookup[current_code] == "database.table"
        and crush.table_code_lookup["database.table"] == current_code
        )


def get_openai_embedding_test():
    from SchemaSubsetter.Crush4SqlSubsetter import get_openai_embedding
    crush = Crush4SqlSubsetter(benchmark=NlSqlBenchmark())
    embedding = get_openai_embedding(
        query="Embed this string please",
        api_key=crush.api_key,
        api_type=crush.api_type,
        endpoint=crush.endpoint,
        api_version=crush.api_version
    )
    return len(embedding) == 1536
    

def make_relation_map_test():
    crush = Crush4SqlSubsetter(benchmark=NlSqlBenchmark())
    r_map = crush._make_relation_map(Schema(
        database="database1",
        tables=[
            SchemaTable(name="table1", columns=[
                TableColumn(name="column1", data_type="text"),
                TableColumn(name="column2", data_type="text")
            ]),
            SchemaTable(name="table2", columns=[
                TableColumn(name="column3", data_type="text"),
                TableColumn(name="column4", data_type="text")
            ])
        ]
    ))
    return r_map == {
        'database1 table1 1000 column1': {
            'table': 'table1', 
            'source': 'database1', 
            'code': '1000'},
        'database1 table1 1000 column2': {
            'table': 'table1', 
            'source': 'database1', 
            'code': '1000'},
        'database1 table2 1001 column3': {
            'table': 'table2', 
            'source': 'database1', 
            'code': '1001'},
        'database1 table2 1001 column4': {
            'table': 'table2', 
            'source': 'database1', 
            'code': '1001'}
        }


def make_super_schema_test():
    bm = NlSqlBenchmark()
    schema = bm.get_active_schema()
    crush = Crush4SqlSubsetter(benchmark=bm)
    super_schema = crush._make_super_schema(schema)
    print(super_schema)
    return super_schema == {
        1000: {
            'name': 'table1', 
            'overview': '', 
            'source': 'database1', 
            'columns': ['column1'], 
            'key_columns': ['column1']
            }
        }


def preprocess_database_test():
    crush = Crush4SqlSubsetter(benchmark=NlSqlBenchmark())
    crush.preprocess_databases()
    relation_map_path = "src/SchemaSubsetter/CRUSH4SQL/processed/database1/database1_relation_map_for_unclean.json"
    return os.path.exists(relation_map_path)


def get_schema_subset_test():
    crush = Crush4SqlSubsetter(benchmark=BirdNlSqlBenchmark())
    question = BirdNlSqlBenchmark().get_active_question()
    subset = crush.get_schema_subset(question)
    return type(subset) == Schema