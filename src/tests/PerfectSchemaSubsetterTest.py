from SchemaSubsetter.PerfectSchemaSubsetter import PerfectSchemaSubsetter
from NlSqlBenchmark.BirdNlSqlBenchmark import BirdNlSqlBenchmark


def get_schema_subset_test():
    pss = PerfectSchemaSubsetter(benchmark=BirdNlSqlBenchmark())
    pss.benchmark.set_active_schema("california_schools")
    correct_result = {
        'tables': [{
            'name': 'satscores', 
            'columns': [{'name': 'cds', 'type': 'text'}, {'name': 'NumTstTakr', 'type': 'integer'}]
            }]
        }
    result = pss.get_schema_subset(
        question="What is the number of SAT test takers of the schools with the highest FRPM count for K-12 students?",
        full_schema=pss.benchmark.get_active_schema()
        )
    return result == correct_result


