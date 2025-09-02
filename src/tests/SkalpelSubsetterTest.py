from SchemaSubsetter.SkalpelSubsetter import SkalpelSubsetter
from SchemaSubsetter.SchemaSubsetterResult import SchemaSubsetterResult
from NlSqlBenchmark.SchemaObjects import (
    Schema, SchemaTable, TableColumn, ForeignKey
)
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark
from NlSqlBenchmark.spider2.Spider2NlSqlBenchmark import Spider2NlSqlBenchmark
from NlSqlBenchmark.snails.SnailsNlSqlBenchmark import SnailsNlSqlBenchmark
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion

def init_test():
    bm = NlSqlBenchmark()
    try:
        ss = SkalpelSubsetter(bm)
        return True
    except:
        return False


def describe_objects_in_question_test():
    bm = BirdNlSqlBenchmark()
    ss = SkalpelSubsetter(bm)
    print(ss._describe_objects_in_question(bm.get_active_question()))
    return False


def preprocess_databases_test():
    bm = NlSqlBenchmark()
    ss = SkalpelSubsetter(bm)
    ss.preprocess_databases(recreate_database=False)
    return True


def get_schema_subset_test():
    bm = BirdNlSqlBenchmark()
    ss = SkalpelSubsetter(bm)
    question = bm.get_active_question()
    subset = ss.get_schema_subset(benchmark_question=question)
    print(question.question)
    print(question.query)
    print(subset.schema_subset)
    return False



def llm_call_with_requests_test():
    bm = NlSqlBenchmark()
    ss = SkalpelSubsetter(bm)
    prompt = """
'You are given a list of tables and columns in DDL form. This list is a subset of the full schema. You previously selected these tables as the most relevant to the question. Your task is now to select the minimal columns for each table required to translate the following natural language query into an SQL query.\n\nDue to context window limitations, I am only able to show you 100 at a time.\n\nYou have already selected the following columns:\nset()\n\nInstructions:\n1. Carefully read the natural language query.\n2. Review the columns in each table.\n3. Identify which columns contain the necessary information to answer the query.\n4. Select only the columns that are essential for constructing the SQL query or for joining tables together.\n5. Provide the columns as table.column strings in a json list enclosed in [square brackets].\n\nExample output format:\n```json\n["users.user_id", "orders.date", "products.name"]\n```\n\nNatural language query:\nHow many different codes are there for each enumeration group?\n\nSchema:\nCREATE TABLE `tlu_Enumerations` (\n  `Enum_Code` NVARCHAR,\n  `Enum_Description` NVARCHAR,\n  `Enum_Group` NVARCHAR,\n  `Sort_Order` INTEGER,\n  `SSMA_TimeStamp` BLOB\n);\n\nSelected columns:'
"""
    try:
        response, tokens = ss.llm.call_with_requests(
            prompt=prompt,
            model="openai/gpt-oss-120b"
        )
        return True
    except:
        pass  
    return False
