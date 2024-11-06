from SchemaSubsetter.PerfectTableSchemaSubsetter import PerfectTableSchemaSubsetter
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark


def get_schema_subset_test():
    pss = PerfectTableSchemaSubsetter(benchmark=BirdNlSqlBenchmark())
    pss.benchmark.set_active_schema("california_schools")
    correct_result = {
        'tables': [
        {'name': 'frpm', 
         'columns': [
             {'name': 'CDSCode', 'type': 'text'}, 
             {'name': 'Academic Year', 'type': 'text'}, 
             {'name': 'County Code', 'type': 'text'}, 
             {'name': 'District Code', 'type': 'integer'}, 
             {'name': 'School Code', 'type': 'text'}, 
             {'name': 'County Name', 'type': 'text'}, 
             {'name': 'District Name', 'type': 'text'}, 
             {'name': 'School Name', 'type': 'text'}, 
             {'name': 'District Type', 'type': 'text'}, 
             {'name': 'School Type', 'type': 'text'}, 
             {'name': 'Educational Option Type', 'type': 'text'}, 
             {'name': 'NSLP Provision Status', 'type': 'text'}, 
             {'name': 'Charter School (Y/N)', 'type': 'integer'}, 
             {'name': 'Charter School Number', 'type': 'text'}, 
             {'name': 'Charter Funding Type', 'type': 'text'}, 
             {'name': 'IRC', 'type': 'integer'}, 
             {'name': 'Low Grade', 'type': 'text'}, 
             {'name': 'High Grade', 'type': 'text'}, 
             {'name': 'Enrollment (K-12)', 'type': 'real'}, 
             {'name': 'Free Meal Count (K-12)', 'type': 'real'}, 
             {'name': 'Percent (%) Eligible Free (K-12)', 'type': 'real'}, 
             {'name': 'FRPM Count (K-12)', 'type': 'real'}, 
             {'name': 'Percent (%) Eligible FRPM (K-12)', 'type': 'real'}, 
             {'name': 'Enrollment (Ages 5-17)', 'type': 'real'}, 
             {'name': 'Free Meal Count (Ages 5-17)', 'type': 'real'}, 
             {'name': 'Percent (%) Eligible Free (Ages 5-17)', 'type': 'real'}, 
             {'name': 'FRPM Count (Ages 5-17)', 'type': 'real'}, 
             {'name': 'Percent (%) Eligible FRPM (Ages 5-17)', 'type': 'real'}, 
             {'name': '2013-14 CALPADS Fall 1 Certification Status', 'type': 'integer'}
            ]
        }, {
            'name': 'satscores', 
            'columns': [
                {'name': 'cds', 'type': 'text'}, 
                {'name': 'rtype', 'type': 'text'}, 
                {'name': 'sname', 'type': 'text'}, 
                {'name': 'dname', 'type': 'text'}, 
                {'name': 'cname', 'type': 'text'}, 
                {'name': 'enroll12', 'type': 'integer'}, 
                {'name': 'NumTstTakr', 'type': 'integer'}, 
                {'name': 'AvgScrRead', 'type': 'integer'}, 
                {'name': 'AvgScrMath', 'type': 'integer'}, 
                {'name': 'AvgScrWrite', 'type': 'integer'}, 
                {'name': 'NumGE1500', 'type': 'integer'}
    ]}]}
    result = pss.get_schema_subset(
        question="What is the number of SAT test takers of the schools with the highest FRPM count for K-12 students?",
        full_schema=pss.benchmark.get_active_schema()
        )
    return result == correct_result