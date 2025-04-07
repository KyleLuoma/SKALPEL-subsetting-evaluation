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
        database='california_schools', 
        tables=[
            SchemaTable(name='frpm', 
                        columns=[
                            TableColumn(name='CDSCode', data_type='text', description='CDSCode'), 
                            TableColumn(name='Academic Year', data_type='text', description='Academic Year'), 
                            TableColumn(name='County Code', data_type='text', description='County Code'), 
                            TableColumn(name='District Code', data_type='integer', description='District Code'), 
                            TableColumn(name='School Code', data_type='text'), 
                            TableColumn(name='County Name', data_type='text', description='County Code '), 
                            TableColumn(name='District Name', data_type='text'), 
                            TableColumn(name='School Name', data_type='text', description='School Name '), 
                            TableColumn(name='District Type', data_type='text', description='District Type'), 
                            TableColumn(name='School Type', data_type='text'), 
                            TableColumn(name='Educational Option Type', data_type='text', description='Educational Option Type'), 
                            TableColumn(name='NSLP Provision Status', data_type='text', description='NSLP Provision Status'), 
                            TableColumn(name='Charter School (Y/N)', data_type='integer', description='Charter School (Y/N)', value_description='"0: N;\n'), 
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
                        foreign_keys=[]
                    ),
            SchemaTable(name='satscores', 
                        columns=[
                            TableColumn(name='cds', data_type='text', description='California Department Schools'), 
                            TableColumn(name='rtype', data_type='text', description='rtype', value_description='unuseful'), 
                            TableColumn(name='sname', data_type='text', description='school name'), 
                            TableColumn(name='dname', data_type='text', description='district segment'), 
                            TableColumn(name='cname', data_type='text', description='county name'), 
                            TableColumn(name='enroll12', data_type='integer', description='enrollment (1st-12nd grade)'), 
                            TableColumn(name='NumTstTakr', data_type='integer', description='Number of Test Takers in this school', value_description='number of test takers in each school\n'), 
                            TableColumn(name='AvgScrRead', data_type='integer', description='average scores in Reading', value_description='average scores in Reading\n'), 
                            TableColumn(name='AvgScrMath', data_type='integer', description='average scores in Math', value_description='average scores in Math\n'), 
                            TableColumn(name='AvgScrWrite', data_type='integer', description='average scores in writing', value_description='average scores in writing\n'), 
                            TableColumn(name='NumGE1500', data_type='integer', description='Number of Test Takers Whose Total SAT Scores Are Greater or Equal to 1500', value_description='"Number of Test Takers Whose Total SAT Scores Are Greater or Equal to 1500\n')
                            ], 
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
    return (
        len(result.tables) == len(correct_result.tables) 
        and len(result.get_table_by_name("satscores").columns) == len(correct_result.get_table_by_name("satscores").columns)
        and len(result.get_table_by_name("frpm").columns) == len(correct_result.get_table_by_name("frpm").columns)
        )