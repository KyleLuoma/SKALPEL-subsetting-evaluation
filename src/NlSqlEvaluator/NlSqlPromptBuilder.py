"""
Copyright 2025 Kyle Luoma

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from NlSqlBenchmark.SchemaObjects import Schema, SchemaTable, TableColumn, ForeignKey
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark



class NlSqlPromptBuilder:

    @staticmethod
    def create_prompt(
        question: BenchmarkQuestion,
        benchmark: NlSqlBenchmark = None,
        use_evidence: bool = False,
        style: str = "ddl",
        sample_values: int = 0,
        column_descriptions: bool = False,
        pk_fk: bool = False,
        prompt_template_file: str = "nl_sql_zero_shot.prompt"
        ) -> str:
        assert style in ["ddl", "openai"]
        if sample_values > 0 and not benchmark:
            sample_values = 0
            raise Warning("Cannot add sample values to prompt with create_prompt without a benchmark object.")
        with open(f"./src/NlSqlEvaluator/prompts/{prompt_template_file}", "rt") as f:
            prompt = f.read()
        if style == "ddl":
            schema_knowledge = "\n".join([table.as_ddl() for table in question.schema.tables])
        elif style == "openai":
            schema_knowledge = NlSqlPromptBuilder.make_openai_nlsql_schema(
                schema=question.schema,
                benchmark=benchmark,
                sample_values=sample_values,
                column_descriptions=column_descriptions,
                pk_fk=pk_fk
            )
        if use_evidence and question.evidence:
            optional_evidence_section = "Here is some additional information about the question and or the schema:\n"
            optional_evidence_section += question.evidence
        else:
            optional_evidence_section = ""

        prompt = prompt.format(
            sql_syntax = question.query_dialect,
            schema_representation = schema_knowledge,
            optional_evidence_section = optional_evidence_section,
            natural_language_question = question.question
        )
        return prompt


    @staticmethod
    def make_openai_nlsql_schema(
            schema: Schema,
            benchmark: NlSqlBenchmark = None,
            sample_values: int = 0, 
            column_descriptions: bool = False, 
            pk_fk: bool = False
            ) -> str:
        schema_knowledge = f"### Database: {schema.database}\n"
        schema_knowledge += f"## Tables with their columns\n"
        for table in schema.tables:
            column_str = ", ".join(f"{c.name} {c.data_type}" for c in table.columns)
            schema_knowledge += f"# {table.name}: {column_str}\n"
            for c in table.columns:
                if sample_values > 0:
                    benchmark.set_active_schema(schema.database)
                    values = benchmark.get_sample_values(table_name=table.name, column_name=c.name, num_values=sample_values)
                    schema_knowledge += f"# {c.name} column sample values: {', '.join(values)}\n"
                if c.description != None and column_descriptions:
                    schema_knowledge += f"# {c.name} description: {c.description}\n"
            if pk_fk and table.foreign_keys != None and benchmark != None:
                benchmark_schema_table = benchmark.get_active_schema(database=schema.database).get_table_by_name(table.name)
                schema_knowledge += f"# {table.name} primary keys: {benchmark_schema_table.primary_keys}\n"
                fk_string = ", ".join([f"{fk.columns} references {fk.references}" for fk in benchmark_schema_table.foreign_keys])
                schema_knowledge += f"# {table.name} foreign keys: {fk_string}\n"
        return schema_knowledge
            
