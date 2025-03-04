from NlSqlBenchmark.spider.SpiderNlSqlBenchmark import SpiderNlSqlBenchmark
from NlSqlBenchmark.QueryResult import QueryResult
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)

spider_dev_databases = [
    'concert_singer', 'pets_1', 'car_1', 'flight_2', 'employee_hire_evaluation', 
    'cre_Doc_Template_Mgt', 'course_teach', 'museum_visit', 'wta_1', 'battle_death', 
    'student_transcripts_tracking', 'tvshow', 'poker_player', 'voter_1', 'world_1', 'orchestra', 
    'network_1', 'dog_kennels', 'singer', 'real_estate_properties'
    ]

def init_test():
    try:
        bm = SpiderNlSqlBenchmark()
        return bm.databases == spider_dev_databases
    except:
        return False


def get_active_question_test():
    bm = SpiderNlSqlBenchmark()
    bm.set_active_question_number(10)
    question = bm.get_active_question()
    return question.question == "Show all countries and the number of singers in each country."


def set_and_get_active_schema_test():
    bm = SpiderNlSqlBenchmark()
    bm.set_active_schema("tvshow")
    schema = bm.get_active_schema()
    return schema == Schema(database="tvshow", tables=[
        SchemaTable(
            name="TV_Channel", 
            columns=[
                TableColumn(name="id", data_type="text"), 
                TableColumn(name="series_name", data_type="text"), 
                TableColumn(name="Country", data_type="text"), 
                TableColumn(name="Language", data_type="text"), 
                TableColumn(name="Content", data_type="text"), 
                TableColumn(name="Pixel_aspect_ratio_PAR", data_type="text"), 
                TableColumn(name="Hight_definition_TV", data_type="text"), 
                TableColumn(name="Pay_per_view_PPV", data_type="text"), 
                TableColumn(name="Package_Option", data_type="text")], 
                primary_keys=["id"], 
                foreign_keys=[]
        ),
        SchemaTable(
            name="TV_series", 
            columns=[
                TableColumn(name="id", data_type="number"), 
                TableColumn(name="Episode", data_type="text"), 
                TableColumn(name="Air_Date", data_type="text"), 
                TableColumn(name="Rating", data_type="text"), 
                TableColumn(name="Share", data_type="number"), 
                TableColumn(name="18_49_Rating_Share", data_type="text"), 
                TableColumn(name="Viewers_m", data_type="text"), 
                TableColumn(name="Weekly_Rank", data_type="number"), 
                TableColumn(name="Channel", data_type="text")], 
                primary_keys=["id"], 
                foreign_keys=[
                    ForeignKey(columns=['Channel'], references=['id'])
                    ]
        ),
        SchemaTable(
            name="Cartoon", 
            columns=[
                TableColumn(name="id", data_type="number"), 
                TableColumn(name="Title", data_type="text"), 
                TableColumn(name="Directed_by", data_type="text"), 
                TableColumn(name="Written_by", data_type="text"), 
                TableColumn(name="Original_air_date", data_type="text"), 
                TableColumn(name="Production_code", data_type="number"), 
                TableColumn(name="Channel", data_type="text")], 
                primary_keys=["id"], 
                foreign_keys=[ForeignKey(columns=['Channel'], references=['id'])]
        )
    ])


def iter_test():
    bm = SpiderNlSqlBenchmark()
    found_databases = set()
    questions = []
    for q in bm:
        found_databases.add(q["schema"]["database"])
        questions.append(1)
    return found_databases == set(spider_dev_databases) and len(questions) == 1034



def execute_query_valid_query_test():
    bm = SpiderNlSqlBenchmark()
    query = "SELECT name ,  country ,  age FROM singer ORDER BY age DESC"
    correct_result = QueryResult(
        result_set={
            'Name': [
                'Joe Sharp', 'John Nizinik', 'Rose White', 'Timbaland', 'Justin Brown', 'Tribal King'
                ], 
            'Country': [
                'Netherlands', 'France', 'France', 'United States', 'France', 'France'
                ], 
            'Age': [52, 43, 41, 32, 29, 25]
            },
        database="concert_singer",
        question=0,
        error_message=None
    )
    res = bm.execute_query(query=query, database="concert_singer")
    return res == correct_result


def execute_query_syntax_error_test():
    bm = SpiderNlSqlBenchmark()
    query = "SELECT name ,  country ,  age FORM singer ORDER BY age DESC"
    correct_result = QueryResult(
        result_set=None, 
        database=None, 
        question=None, 
        error_message='near "singer": syntax error'
    )
    res = bm.execute_query(query=query, database="concert_singer")
    return res == correct_result


def get_sample_values_test():
    bm = SpiderNlSqlBenchmark()
    sample_values = bm.get_sample_values(
        database="concert_singer",
        table_name="singer",
        column_name="name"
    )
    return sample_values == ['Joe Sharp', 'Timbaland']
