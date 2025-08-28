from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark, BenchmarkQuestion
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
import pandas as pd
from tqdm import tqdm
from util.StringObjectParser import StringObjectParser
from SchemaSubsetter.Skalpel import LLM

class NlSqlEvaluator:

    def __init__(
            self,
            benchmark: NlSqlBenchmark,
            subset_df: pd.DataFrame = None
            ):
        self.benchmark = benchmark
        self.subsets = self._get_subsets_from_subset_df(subset_df=subset_df)
        self.llm = LLM.OpenAIRequestLLM(request_url=LLM.OpenAIRequestLLM.REQUEST_URL)
        


    def generate_sql_from_subset_df(
            self, 
            subset_df: pd.DataFrame,
            llm_model: str = None
            ) -> pd.DataFrame:
        question_list = self._get_subsets_from_subset_df(subset_df)
        generated_queries = []
        gold_queries = []
        tokens = []
        equivalent = []
        non_eq_reason = []
        for question in tqdm(question_list, desc=f"Generating sql queries."):
            sql, token_count = self._nl_to_sql(
                question=question,
                model=llm_model
            )
            generated_queries.append(sql)
            gold_queries.append(question.query)
            tokens.append(token_count)
            evaluation = self.benchmark.compare_gold_to_generated_query(
                benchmark_question=question, generated_query=sql
            )
            equivalent.append(evaluation["equivalent"])
            non_eq_reason.append(evaluation["reason"])

        sql_df = subset_df[["subsetting_method", "comments", "benchmark", "database" "question_number"]]
        sql_df["generated_query"] = generated_queries
        sql_df["nl_sql_tokens"] = tokens
        sql_df["result_set_match"] = equivalent
        sql_df["non_match_reason"] = non_eq_reason
        return sql_df



    def _nl_to_sql(self, 
                  question: BenchmarkQuestion, 
                  model: str = None,
                  prompt_file: str = "nl_sql_zero_shot.prompt"
                  ) -> tuple[str, int]:
        prompt = self._create_prompt(question, prompt_file)
        sql, token_count = self.llm.call_llm(prompt=prompt, model=model)
        return sql, token_count



    def _create_prompt(self, question: BenchmarkQuestion, prompt_file: str = "nl_sql_zero_shot.prompt") -> str:
        with open(f"./src/NlSqlEvaluator/prompts{prompt_file}", "rt") as f:
            prompt = f.read()
        schema_knowledge = "\n".join([table.as_ddl() for table in question.schema.tables])
        prompt = prompt.format(
            sql_syntax = question.query_dialect,
            schema_representation = schema_knowledge,
            natural_language_question = question.question
        )
        return prompt



    def _get_subsets_from_subset_df(self, subset_df: pd.DataFrame) -> list[BenchmarkQuestion]:
        question_list = []
        for row in subset_df.itertuples():
            correct_tables: set = StringObjectParser.string_to_python_object(row.correct_tables, True)
            missing_tables: set = StringObjectParser.string_to_python_object(row.missing_tables, True)
            extra_tables: set = StringObjectParser.string_to_python_object(row.extra_tables, True)
            correct_columns: set = StringObjectParser.string_to_python_object(row.correct_columns, True)
            missing_columns: set = StringObjectParser.string_to_python_object(row.missing_columns, True)
            extra_columns: set = StringObjectParser.string_to_python_object(row.extra_columns, True)
            subset_tables = correct_tables.union(missing_tables).union(extra_tables)
            subset_columns = correct_columns.union(missing_columns).union(extra_columns)

            self.benchmark.set_active_schema(row.database)
            self.benchmark.set_active_question_number(row.question_number)
            schema = self.benchmark.get_active_schema()
            subset_schema = Schema(
                dabase=schema.database, tables=[]
            )
            for table in schema.tables:
                if table.name not in subset_tables:
                    continue
                subset_table = SchemaTable(name=table.name, columns=[])
                for column in table.columns:
                    if f"{table.name}.{column.name}" not in subset_columns:
                        continue
                    subset_table.columns.append(column)
                subset_schema.append(subset_table)
            subset_question = self.benchmark.get_active_question()
            subset_question.schema = subset_schema
            question_list.append(subset_question)
        return question_list



