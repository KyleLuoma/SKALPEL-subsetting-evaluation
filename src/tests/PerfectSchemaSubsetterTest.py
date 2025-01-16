from SchemaSubsetter.PerfectSchemaSubsetter import PerfectSchemaSubsetter
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion


def get_schema_subset_test():
    pss = PerfectSchemaSubsetter(benchmark=BirdNlSqlBenchmark())
    pss.benchmark.set_active_schema("california_schools")
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
        query="SELECT...",
        question_number=0,
        schema=pss.benchmark.get_active_schema()
    ))
    return result == correct_result


