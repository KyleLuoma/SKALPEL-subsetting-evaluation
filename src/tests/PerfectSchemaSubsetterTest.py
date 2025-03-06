from SchemaSubsetter.Perfect.PerfectSchemaSubsetter import PerfectSchemaSubsetter
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark
from NlSqlBenchmark.snails.SnailsNlSqlBenchmark import SnailsNlSqlBenchmark
from NlSqlBenchmark.spider2.Spider2NlSqlBenchmark import Spider2NlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion


def get_schema_subset_test():
    pss = PerfectSchemaSubsetter()
    benchmark = BirdNlSqlBenchmark()
    benchmark.set_active_schema("california_schools")
    correct_result = Schema(
        database="california_schools",
        tables=[
            SchemaTable(
                name='frpm', 
                columns=[
                    TableColumn(name='County Name', data_type='text'),
                    TableColumn(name='Enrollment (K-12)', data_type='real'),
                    TableColumn(name='Free Meal Count (K-12)', data_type='real')
                    ],
                primary_keys=[],
                foreign_keys=[]
                )
        ]
    )
    result = pss.get_schema_subset(BenchmarkQuestion(
        question="What is the highest eligible free rate for K-12 students in the schools in Alameda County?",
        query="SELECT `Free Meal Count (K-12)` / `Enrollment (K-12)` FROM frpm WHERE `County Name` = 'Alameda' ORDER BY (CAST(`Free Meal Count (K-12)` AS REAL) / `Enrollment (K-12)`) DESC LIMIT 1",
        query_dialect=benchmark.sql_dialect,
        question_number=0,
        schema=benchmark.get_active_schema()
    ))
    print(result)
    return result == correct_result


def spider_2_query_test():
    query_file = "./src/tests/sql/PerfectSchemaSubsetter/spider2.sql"
    with open(query_file, "rt") as f:
        query = f.read()
    pss = PerfectSchemaSubsetter()
    benchmark = Spider2NlSqlBenchmark()
    subset = pss.get_schema_subset(BenchmarkQuestion(
        question="foo",
        question_number=0,
        query=query,
        query_dialect="postgresql",
        schema=benchmark.get_active_schema(database="ga360")
    ))
    return type(subset) == Schema




