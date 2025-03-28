from NlSqlBenchmark.spider2.Spider2NlSqlBenchmark import Spider2NlSqlBenchmark
from NlSqlBenchmark.QueryResult import QueryResult
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
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


def iter_test():
    bm = Spider2NlSqlBenchmark()
    bm.schema_pickling_disabled = False
    itercount = 0
    iter_questions = set()
    for question in bm:
        iter_questions.add(question.question)
        itercount += 1
    return itercount == (256 - len(bm.exclude_from_eval))


def get_sample_values_test():
    bm = Spider2NlSqlBenchmark()
    values = bm.get_sample_values(
        table_name="bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20201106",
        column_name="event_name"
    )
    return values == ["user_engagement", "page_view"]


def set_and_get_active_schema_test():
    bm = Spider2NlSqlBenchmark()
    bm.set_active_schema("PATENTS")
    schema = bm.get_active_schema()
    return schema.database == "PATENTS"


def query_snowflake_test():
    bm = Spider2NlSqlBenchmark()
    result = bm.query_snowflake(
        query = """
        SELECT 
            "from_address" AS "address",
            -(CAST("receipt_gas_used" AS NUMBER) * CAST("gas_price" AS NUMBER)) AS "value"
        FROM "ETHEREUM_BLOCKCHAIN"."ETHEREUM_BLOCKCHAIN"."TRANSACTIONS"
        LIMIT 5;
        """,
        database="ETHEREUM_BLOCKCHAIN"
    )
    return (
        set(result.result_set.keys()) == {"address", "value"} 
        and len(result.result_set["address"]) == 5
        )


def query_sqlite_test():
    query = "select NAME, productnumber from product limit 5;"
    bm = Spider2NlSqlBenchmark()
    result = bm.query_sqlite(
        query=query,
        database="AdventureWorks"
    )
    return (
        set(result.result_set.keys()) == {"NAME", "productnumber"} 
        and len(result.result_set["NAME"]) == 5
        )


def query_bigquery_test():
    query = """
    DECLARE start_date STRING DEFAULT '20170201';
    DECLARE end_date STRING DEFAULT '20170228';
    SELECT
        fullvisitorid,
        MIN(date) AS date_first_visit
    FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE
       _TABLE_SUFFIX BETWEEN start_date AND end_date
    GROUP BY fullvisitorid
    LIMIT 5
    """
    bm = Spider2NlSqlBenchmark()
    result = bm.query_bigquery(
        query=query,
        database=""
    )
    return (
        set(result.result_set.keys()) == {"fullvisitorid", "date_first_visit"}
        and len(result.result_set["fullvisitorid"]) == 5
    )


def execute_query_sqlite_db_test():
    bm = Spider2NlSqlBenchmark()
    try:
        bm.execute_query(query="select 1", database="northwind")
        return True
    except Exception as e:
        return False
    

def execute_query_snowflake_db_test():
    bm = Spider2NlSqlBenchmark()
    try:
        bm.execute_query(query="select 1", database="ETHEREUM_BLOCKCHAIN")
        return True
    except Exception as e:
        return False
    

def execute_query_bigquery_db_test():
    bm = Spider2NlSqlBenchmark()
    try:
        bm.execute_query(query="select 1", database="bbc")
        return True
    except Exception as e:
        return False