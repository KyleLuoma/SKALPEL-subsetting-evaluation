from NlSqlBenchmark.spider2.Spider2NlSqlBenchmark import Spider2NlSqlBenchmark
from NlSqlBenchmark.QueryResult import QueryResult
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)


def init_test():
    try:
        bm = Spider2NlSqlBenchmark()
        return True
    except Exception as e:
        print(e)
        raise e
        return False
    

def get_active_schema_test():
    bm = Spider2NlSqlBenchmark()
    schema = bm.get_active_schema()
    return len(schema.tables) == 92


# def iter_test():
#     bm = Spider2NlSqlBenchmark()
#     itercount = 0
#     iter_questions = set()
#     for question in bm:
#         iter_questions.add(question.question)
#         itercount += 1
#     return itercount == 256


def get_sample_values_test():
    bm = Spider2NlSqlBenchmark()
    values = bm.get_sample_values(
        table_name="bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20201106",
        column_name="event_name"
    )
    return values == ["user_engagement", "page_view"]


def set_and_get_active_schema_test():
    bm = Spider2NlSqlBenchmark()
    bm.set_active_schema("Pagila")
    schema = bm.get_active_schema()
    for table in schema.tables:
        print(table)
    return schema.database == "Pagila"