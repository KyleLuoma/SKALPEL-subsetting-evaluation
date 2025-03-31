from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark

from SchemaSubsetter.CRUSH4SQL.demo.utils.utils import ndap_pipeline

import json

class Crush4SqlSubsetter(SchemaSubsetter):

    name = "crush4sql"

    def __init__(self, benchmark):
        self.name = Crush4SqlSubsetter.name
        self.correct_txt_sql_pairs = {}
        self.api_type = 'python'
        with open("./.loca/openai.json") as f:
            open_ai_info = json.load(f)
        self.api_key = open_ai_info["api_key"]
        self.endpoint = None
    

    def get_schema_subset(self, benchmark_question: BenchmarkQuestion) -> Schema:
        hallucinated_schema, predicted_schema, predicted_sql = ndap_pipeline(
                    benchmark_question.question, 
                    self.api_type, 
                    self.api_key, 
                    self.endpoint, 
                    self.api_version, 
                    self.correct_txt_sql_pairs,
                    generate_sql_query=False
                )