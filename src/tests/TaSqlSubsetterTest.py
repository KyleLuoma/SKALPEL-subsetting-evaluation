from SchemaSubsetter.TaSqlSubsetter import TaSqlSubsetter
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark
from NlSqlBenchmark.snails.SnailsNlSqlBenchmark import SnailsNlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema
)
import json
from pathlib import Path

def init_test():
    try:
        bm = NlSqlBenchmark()
        tasql = TaSqlSubsetter(bm)
        return True
    except:
        return False
    

def preprocess_databases_test():
    bm = NlSqlBenchmark()
    tasql = TaSqlSubsetter(bm)
    perf_times = tasql.preprocess_databases(exist_ok=True)
    print(perf_times)
    return "database1" in perf_times.keys() and type(perf_times["database1"]) == float


def make_table_json_test():
    bm = BirdNlSqlBenchmark()
    tasql = TaSqlSubsetter(benchmark=bm)
    try:
        tasql.dummy_db_root_path.mkdir(exist_ok=False)
    except FileExistsError as e:
        pass
    tasql._make_table_json()
    table_json = json.load(open(tasql.dummy_db_root_path / "dev_tables.json", "r"))
    return len(table_json) == 11


def make_question_json_test():
    bm = BirdNlSqlBenchmark()
    tasql = TaSqlSubsetter(benchmark=bm)
    try:
        tasql.dummy_db_root_path.mkdir(exist_ok=False)
    except FileExistsError as e:
        pass
    tasql._make_question_json()
    question_json = json.load(open(tasql.dummy_db_root_path / "dev.json", "r"))
    return len(question_json) == 1534


def get_schema_subset_test():
    bm = BirdNlSqlBenchmark()
    tasql = TaSqlSubsetter(benchmark=bm)
    tasql.preprocess_databases(skip_already_processed=True)
    result = tasql.get_schema_subset(bm.get_active_question())
    print(result.schema_subset)
    print(result.prompt_tokens)
    return type(result.schema_subset) == Schema