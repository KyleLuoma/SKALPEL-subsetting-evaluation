from SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.DINSQL import DINSQL
from NlSqlBenchmark import NlSqlBenchmark
import openai
import json
import os
import time
from collections import defaultdict


class DinSqlSubsetter(SchemaSubsetter.SchemaSubsetter):

    name = "DINSQL"

    def __init__(
            self,
            benchmark: NlSqlBenchmark.NlSqlBenchmark
            ):
        self.benchmark = benchmark
        with open("./.local/openai.json", "r") as openai_file:
            openai_info = json.loads(openai_file.read())
        self.openai_key = openai_info["api_key"]
        openai.api_key = self.openai_key
        # self.schema_linking_prompt = DINSQL.schema_linking_prompt

    def get_schema_subset(
            self,
            question: str,
            full_schema: dict,
            print_prompt: bool = False
    ) -> dict:
        prompt = self.schema_linking_prompt_maker(
            question=question,
            schema=full_schema
        )
        if print_prompt:
            print("---- Subsetting prompt ----")
            print(prompt)
            print("---- Subsetting prompt ----")
        schema_links = None
        while schema_links is None:
            try:
                schema_links = self.GPT4_generation(prompt=prompt)
            except IndexError:
                time.sleep(3)
                print(".")
                pass
        try:
            schema_links = schema_links.split("Schema_links: ")[1]
        except:
            print("Slicing error for the schema_linking module")
            schema_links = "[]"
        schema_links = schema_links.replace("[", "").replace("]", "")
        schema_links = schema_links.split(",")
        schema_subset = {"tables": []}
        added_table_names = set()
        added_column_names = set()

        table_column_pairs = []
        for link in schema_links:
            if "." in link and "=" not in link: #indicates table.column pair
                table_column_pairs.append(
                    (link.split(".")[0], link.split(".")[1])
                )
            elif "=" in link: #indicates dependency constraint
                left = link.split("=")[0].strip()
                right = link.split("=")[1].strip()
                table_column_pairs += [
                    (left.split(".")[0], left.split(".")[1]),
                    (right.split(".")[0], right.split(".")[1])
                ]
        for pair in table_column_pairs:
                table = pair[0]
                column = pair[1]
                if table not in added_table_names:
                    added_table_names.add(table)
                    added_column_names.add((table, column))
                    schema_subset["tables"].append(
                        {
                            "name": table, 
                            "columns": [{"name": column, "type": None}], 
                            "primary_keys": [], 
                            "foreign_keys": []
                        }
                    )
                else:
                    for ix, subset_table in enumerate(schema_subset["tables"]):
                        if table == subset_table["name"] and (table, column) not in added_column_names:
                            added_column_names.add((table, column))
                            schema_subset["tables"][ix]["columns"].append(
                                {"name": column, "type": None}
                            )
        return schema_subset
        


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
                output += f"{table['name']}.{fk['columns'][0]} = {fk['references'][0]}.{fk['references'][1][0]},"
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
    


    def GPT4_generation(self, prompt):

        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            n = 1,
            stream = False,
            temperature=0.0,
            max_tokens=600,
            top_p = 1.0,
            frequency_penalty=0.0,
            presence_penalty=0.0,
            stop = ["Q:"]
        )
        return response['choices'][0]['message']['content']