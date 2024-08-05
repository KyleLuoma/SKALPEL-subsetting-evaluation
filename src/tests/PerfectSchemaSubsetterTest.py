from SchemaSubsetter.PerfectSchemaSubsetter import PerfectSchemaSubsetter
from NlSqlBenchmark.BirdNlSqlBenchmark import BirdNlSqlBenchmark


def get_schema_subset_test():
    pss = PerfectSchemaSubsetter(benchmark=BirdNlSqlBenchmark())
    pss.benchmark.set_active_schema("california_schools")
    correct_result = {
        'tables': [
            {
                'name': 'frpm', 
                'columns': [
                    {'name': 'County Name', 'type': 'text'}, 
                    {'name': 'Enrollment (K-12)', 'type': 'real'}, 
                    {'name': 'Free Meal Count (K-12)', 'type': 'real'}
    ]}]}
    result = pss.get_schema_subset(
        question="What is the highest eligible free rate for K-12 students in the schools in Alameda County?",
        full_schema=pss.benchmark.get_active_schema()
        )
    return result == correct_result


