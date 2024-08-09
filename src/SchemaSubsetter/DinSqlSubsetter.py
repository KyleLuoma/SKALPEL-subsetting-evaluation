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
        prompt = self.schema_linking_prompt_maker(
            question=question,
            schema=full_schema
        )
        print(prompt)
        din_schema = self.transform_schema_to_dinsql_format(full_schema)
        return din_schema
        


    def transform_schema_to_dinsql_format(self, schema: dict):
        output = ""
        for table in schema["tables"]:
            output += "Table " + table["name"] + ", columns = [*,"
            for column in table["columns"]:
                output += column["name"] + ","
            output = output[:-1]
            output += "]\n"
        return output
    


    def transform_dependencies_to_dinsql_format(self, schema: dict):
        output = "["
        for table in schema["tables"]:
            for fk in table["foreign_keys"]:
                output += f"{table["name"]}.{fk["columns"][0]} = {fk["references"][0]}.{fk["references"][1][0]},"
        output = output[:-1] + "]"
        return output
    


    def schema_linking_prompt_maker(self, question: str, schema: dict):
        instruction = "# Find the schema_links for generating SQL queries for each question based on the database schema and Foreign keys.\n"
        fields = self.transform_schema_to_dinsql_format(schema=schema)
        foreign_keys = "Foreign_keys = " + "\n"
        foreign_keys += self.transform_dependencies_to_dinsql_format(schema=schema)
        prompt = instruction + DINSQL.schema_linking_prompt + fields + foreign_keys
        prompt += "\nQ: \"" + question + """"\nA: Let’s think step by step."""
        return prompt