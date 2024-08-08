from SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.DINSQL import DINSQL
from NlSqlBenchmark import NlSqlBenchmark


class DinSqlSubsetter(SchemaSubsetter.SchemaSubsetter):

    def __init__(
            self,
            benchmark: NlSqlBenchmark.NlSqlBenchmark
            ):
        self.benchmark = benchmark
        # self.schema_linking_prompt = DINSQL.schema_linking_prompt

    def get_schema_subset(
            self,
            question: str,
            full_schema: dict
    ) -> dict:
        din_schema = self.transform_schema_to_dinsql_format(full_schema)
        print(din_schema)
        


    def transform_schema_to_dinsql_format(self, schema: dict):
        output = ""
        for table in schema["tables"]:
            output += "Table " + table["name"] + ", columns = [*,"
            for column in table["columns"]:
                output += column["name"] + ","
            output = output[:-1]
            output += "]\n"
        return output
    


    def schema_linking_prompt_maker(self, question: str, schema: dict):
        instruction = "# Find the schema_links for generating SQL queries for each question based on the database schema and Foreign keys.\n"
        fields = self.transform_schema_to_dinsql_format(schema=schema)
        foreign_keys = "Foreign_keys = " + "\n"
        prompt = instruction + DINSQL.schema_linking_prompt + fields + foreign_keys
        prompt += "Q: \"" + question + """"\nA: Let’s think step by step."""
        return prompt