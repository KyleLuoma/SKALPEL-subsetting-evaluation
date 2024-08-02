from SchemaSubsetter.PerfectSchemaSubsetter import PerfectSchemaSubsetter
from NlSqlBenchmark.BirdNlSqlBenchmark import BirdNlSqlBenchmark


def get_schema_subset_test():
    pss = PerfectSchemaSubsetter(BirdNlSqlBenchmark())
    correct_result = {
        'tables': [
            {
                'name': 'gasstations', 
                'columns': [
                    {'name': 'GasStationID', 'type': 'integer'}, 
                    {'name': 'Country', 'type': 'text'}, 
                    {'name': 'Segment', 'type': 'text'}
                    ]
                }
            ]
        }
    result = pss.get_schema_subset(
        question='What is the post ID and the comments commented in the post titled by "Group differences on a five point Likert item"?',
        full_schema=pss.benchmark.get_active_schema()
        )
    return result == correct_result


