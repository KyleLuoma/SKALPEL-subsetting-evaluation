from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark, BenchmarkQuestion
from NlSqlBenchmark.QueryResult import QueryResult
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from NlSqlEvaluator.NlSqlPromptBuilder import NlSqlPromptBuilder as prompt_builder
import pandas as pd
from tqdm import tqdm
from util.StringObjectParser import StringObjectParser
from SchemaSubsetter.Skalpel import LLM
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema, SchemaTable, TableColumn
)
import pickle
import os
from datetime import datetime
import time

class NlSqlEvaluator:

    def __init__(
            self,
            benchmark: NlSqlBenchmark
            ):
        self.benchmark = benchmark
        self.llm = LLM.OpenAIRequestLLM(request_url=LLM.OpenAIRequestLLM.REQUEST_URL)
        


    def generate_sql_from_subset_df_or_benchmark(
            self, 
            subset_df: pd.DataFrame,
            llm_model: str = None,
            source_filename: str = "",
            recover_previous: bool = False
            ) -> pd.DataFrame:
        if subset_df is None:
            question_list = [q for q in self.benchmark]
        else:
            question_list = self._get_subsets_from_subset_df(subset_df)
        database = []
        question_number = []
        generated_queries = []
        gold_queries = []
        tokens = []
        equivalent = []
        non_eq_reason = []
        for question in tqdm(question_list, desc=f"Generating sql queries using {llm_model}."):
            filename_model = llm_model.replace(".", "") if llm_model else ""
            pickle_filename = f"./.nlsql_recovery/{source_filename.replace('.xlsx', '')}_{self.benchmark.name}_{question.schema.database}_{question.question_number}_{filename_model.replace('/', '-')}.pkl"
            database.append(question.schema.database)
            question_number.append(question.question_number)
            if recover_previous:
                try:
                    with open(pickle_filename, "rb") as f:
                        data = pickle.load(f)
                    generated_queries.append(data["sql"])
                    gold_queries.append(question.query)
                    tokens.append(data["token_count"])
                    equivalent.append(data["evaluation"]["equivalent"])
                    non_eq_reason.append(data["evaluation"]["reason"])
                    continue
                except FileNotFoundError:
                    pass
            if len(question.schema.tables) == 0:
                sql = ""
                token_count = -1
                prompt = "No prompt generated due to empty subset"
            else:
                sql, token_count, prompt = self._nl_to_sql(
                    question=question,
                    model=llm_model
                )
                if "pro" in llm_model:
                    time.sleep(30)
            generated_queries.append(sql)
            gold_queries.append(question.query)
            tokens.append(token_count)
            evaluation, gold_results, generated_results = self.benchmark.compare_gold_to_generated_query(
                benchmark_question=question, generated_query=sql
            )
            equivalent.append(evaluation["equivalent"])
            non_eq_reason.append(evaluation["reason"])
            data_to_pickle = {
                "sql": sql,
                "token_count": token_count,
                "evaluation": evaluation
            }
            self._log_attempt(
                question=question,
                subset_source_filename=source_filename,
                model=llm_model,
                prompt=prompt,
                token_count=token_count,
                sql=sql,
                evaluation=evaluation,
                gold_results=gold_results,
                generated_results=generated_results
            )
            with open(pickle_filename, "wb") as f:
                pickle.dump(data_to_pickle, f)

        if subset_df is not None:
            sql_df = subset_df[["database", "question_number"]]
        else:
            sql_df = pd.DataFrame({"database": database, "question_number": question_number})
        sql_df["nl_sql_model"] = llm_model
        sql_df["generated_query"] = generated_queries
        sql_df["gold_query"] = gold_queries
        sql_df["nl_sql_tokens"] = tokens
        sql_df["result_set_match"] = equivalent
        sql_df["non_match_reason"] = non_eq_reason
        return sql_df



    def _nl_to_sql(self, 
                  question: BenchmarkQuestion, 
                  model: str = None,
                  prompt_file: str = "nl_sql_zero_shot.prompt"
                  ) -> tuple[str, int]:
        prompt = self._create_prompt(
            question=question, 
            prompt_file=prompt_file,
            benchmark=self.benchmark,
            use_evidence=True,
            style="openai",
            sample_values=0,
            column_descriptions=False,
            pk_fk=True
            )
        if model != None:
            llm = LLM.LLMFactory.build_llm_for_model(model)
        else:
            llm = self.llm
        sql, token_count = llm.call_llm(prompt=prompt, model=model)
        sql = sql.split("```sql")[-1].replace("```", "").strip()
        return sql, token_count, prompt



    def _create_prompt(
            self, 
            question: BenchmarkQuestion, 
            prompt_file: str = "nl_sql_zero_shot.prompt",
            benchmark: NlSqlBenchmark = None,
            use_evidence: bool = False,
            style: str = "ddl",
            sample_values: int = 0,
            column_descriptions: bool = False,
            pk_fk: bool = False,
            ) -> str:
        prompt = prompt_builder.create_prompt(
            question=question,
            prompt_template_file=prompt_file,
            benchmark=benchmark,
            use_evidence=use_evidence,
            style=style,
            sample_values=sample_values,
            column_descriptions=column_descriptions,
            pk_fk=pk_fk
        )
        return prompt



    def _log_attempt(
            self, 
            question: BenchmarkQuestion,
            subset_source_filename: str, 
            model: str, 
            prompt: str, 
            token_count: int, 
            sql: str,
            evaluation: dict,
            gold_results: QueryResult,
            generated_results: QueryResult
            ):
        log_directory = "./src/NlSqlEvaluator/log/"
        log_filename = f"{subset_source_filename.replace('.xlsx', '')}_{question.schema.database}_{question.question_number}_{model.replace('/', '-')}.log"
        timestamp = datetime.now().strftime("%Y%m%d")
        model_log_directory = os.path.join(log_directory, model.replace("/","-"), timestamp)
        try:
            os.makedirs(model_log_directory, exist_ok=False)
        except OSError:
            pass
        log_path = os.path.join(model_log_directory, log_filename)
        with open(log_path, "w") as log_file:
            log_file.write(f"##### Question Number #####\n{question.question_number}\n")
            log_file.write(f"##### Question #####\n{question.question}\n")
            log_file.write(f"##### Prompt #####\n{prompt}\n")
            log_file.write(f"##### Token count #####\n{token_count}\n")
            log_file.write(f"##### Gold SQL #####\n{question.query}\n")
            log_file.write(f"##### Gold Result\n{gold_results.result_set}\n")
            log_file.write(f"##### Generated SQL #####\n{sql}\n")
            log_file.write(f"##### Generated Result\n{generated_results.result_set}\n")
            log_file.write(f"##### Evaluation #####\n{evaluation}\n")
            



    def _get_subsets_from_subset_df(self, subset_df: pd.DataFrame) -> list[BenchmarkQuestion]:
        question_list = []
        for row in subset_df.itertuples():
            correct_tables: set = StringObjectParser.string_to_python_object(row.correct_tables.lower(), True)
            extra_tables: set = StringObjectParser.string_to_python_object(row.extra_tables.lower(), True)
            correct_columns: set = StringObjectParser.string_to_python_object(row.correct_columns.lower(), True)
            extra_columns: set = StringObjectParser.string_to_python_object(row.extra_columns.lower(), True)
            subset_tables = correct_tables.union(extra_tables)
            subset_columns = correct_columns.union(extra_columns)

            self.benchmark.set_active_schema(row.database)
            self.benchmark.set_active_question_number(row.question_number)
            schema = self.benchmark.get_active_schema()
            subset_schema = Schema(
                database=schema.database, tables=[]
            )
            for table in schema.tables:
                if table.name.lower() not in subset_tables:
                    continue
                subset_table = SchemaTable(name=table.name, columns=[])
                for column in table.columns:
                    if f"{table.name.lower()}.{column.name.lower()}" not in subset_columns:
                        continue
                    subset_table.columns.append(column)
                subset_schema.tables.append(subset_table)
            for table in subset_tables:
                if not schema.table_exists(table):
                    halucinated_table = SchemaTable(name=table, columns=[])
                    for column in subset_columns:
                        if ".".join(column.split(".")[-1]) == table:
                            halucinated_table.columns.append(TableColumn(
                                name=column.split(".")[-1],
                                data_type="Unknown Type"
                            ))
                    subset_schema.tables.append(halucinated_table)


            subset_question = self.benchmark.get_active_question()
            subset_question.schema = subset_schema
            question_list.append(subset_question)
        return question_list



