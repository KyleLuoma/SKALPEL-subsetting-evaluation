from SchemaSubsetter.Perfect.PerfectTableSchemaSubsetter import PerfectTableSchemaSubsetter
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion


def get_schema_subset_test():
    pss = PerfectTableSchemaSubsetter()
    benchmark = BirdNlSqlBenchmark()
    benchmark.set_active_schema("california_schools")
    correct_result = Schema(
        database="california_schools",
        tables=[
            SchemaTable(name='frpm', columns=[
                TableColumn(name='CDSCode', data_type='text'),
                TableColumn(name='Academic Year', data_type='text'),
                TableColumn(name='County Code', data_type='text'),
                TableColumn(name='District Code', data_type='integer'),
                TableColumn(name='School Code', data_type='text'),
                TableColumn(name='County Name', data_type='text'),
                TableColumn(name='District Name', data_type='text'),
                TableColumn(name='School Name', data_type='text'),
                TableColumn(name='District Type', data_type='text'),
                TableColumn(name='School Type', data_type='text'),
                TableColumn(name='Educational Option Type', data_type='text'),
                TableColumn(name='NSLP Provision Status', data_type='text'),
                TableColumn(name='Charter School (Y/N)', data_type='integer'),
                TableColumn(name='Charter School Number', data_type='text'),
                TableColumn(name='Charter Funding Type', data_type='text'),
                TableColumn(name='IRC', data_type='integer'),
                TableColumn(name='Low Grade', data_type='text'),
                TableColumn(name='High Grade', data_type='text'),
                TableColumn(name='Enrollment (K-12)', data_type='real'),
                TableColumn(name='Free Meal Count (K-12)', data_type='real'),
                TableColumn(name='Percent (%) Eligible Free (K-12)', data_type='real'),
                TableColumn(name='FRPM Count (K-12)', data_type='real'),
                TableColumn(name='Percent (%) Eligible FRPM (K-12)', data_type='real'),
                TableColumn(name='Enrollment (Ages 5-17)', data_type='real'),
                TableColumn(name='Free Meal Count (Ages 5-17)', data_type='real'),
                TableColumn(name='Percent (%) Eligible Free (Ages 5-17)', data_type='real'),
                TableColumn(name='FRPM Count (Ages 5-17)', data_type='real'),
                TableColumn(name='Percent (%) Eligible FRPM (Ages 5-17)', data_type='real'),
                TableColumn(name='2013-14 CALPADS Fall 1 Certification Status', data_type='integer')
                ],
                primary_keys=[],
                foreign_keys=[]
            ),
            SchemaTable(name='satscores', columns=[
                TableColumn(name='cds', data_type='text'),
                TableColumn(name='rtype', data_type='text'),
                TableColumn(name='sname', data_type='text'),
                TableColumn(name='dname', data_type='text'),
                TableColumn(name='cname', data_type='text'),
                TableColumn(name='enroll12', data_type='integer'),
                TableColumn(name='NumTstTakr', data_type='integer'),
                TableColumn(name='AvgScrRead', data_type='integer'),
                TableColumn(name='AvgScrMath', data_type='integer'),
                TableColumn(name='AvgScrWrite', data_type='integer'),
                TableColumn(name='NumGE1500', data_type='integer')
                ],
                primary_keys=[],
                foreign_keys=[]
            )
    ])
    result = pss.get_schema_subset(BenchmarkQuestion(
        question="What is the number of SAT test takers of the schools with the highest FRPM count for K-12 students?",
        query="SELECT NumTstTakr FROM satscores WHERE cds = ( SELECT CDSCode FROM frpm ORDER BY `FRPM Count (K-12)` DESC LIMIT 1 )",
        query_dialect="sqlite",
        question_number=8,
        schema=benchmark.get_active_schema()
    ))
    return result == correct_result