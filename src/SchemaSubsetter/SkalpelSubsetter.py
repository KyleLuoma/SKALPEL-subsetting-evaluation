from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.TaSqlSubsetter import TaSqlSubsetter
from SchemaSubsetter.SchemaSubsetterResult import SchemaSubsetterResult
from NlSqlBenchmark.SchemaObjects import (
    Schema, SchemaTable, TableColumn, ForeignKey
)
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
import warnings
from tqdm import tqdm
import time
from collections import defaultdict

import SchemaSubsetter.Skalpel.LLM as LLM
from SchemaSubsetter.Skalpel.VectorSearch import VectorSearch
from SchemaSubsetter.Skalpel.SkalpelVectorSearchResults import VectorSearchResults, WordIdentifierDistance
import logging

class SkalpelSubsetter(SchemaSubsetter):

    name = "skalpel"

    def __init__(self, benchmark: NlSqlBenchmark):
        self.name = SkalpelSubsetter.name
        self.benchmark = benchmark
        self.llm = LLM.OpenAIRequestLLM(
            # request_url="https://api.openai.com/v1/chat/completions"
            request_url=LLM.OpenAIRequestLLM.REQUEST_URL
            )
        self.llm_model = "openai/gpt-oss-120b"
        # self.llm_model = "gpt-4.1"
        # self.llm_model = LLM.OpenAIRequestLLM.DEFAULT_MODEL
        self.vector_search = VectorSearch(
            benchmark_name=self.benchmark.name,
            db_host="cdas2"
        )
        self.logger = logging.getLogger(__name__)
        self.table_description_cache: dict[tuple[str, str], str] = {}
        self.tasql = TaSqlSubsetter(benchmark=benchmark)
        


    def preprocess_databases(
            self,
            recreate_database: bool = True,
            **args
        ) -> dict:
        warnings.filterwarnings("ignore")
        processing_times = {}
        if recreate_database:
            self.vector_search.recreate_db()
        for db_name in self.benchmark.databases:
            s_time = time.perf_counter()
            self.benchmark.set_active_schema(db_name)
            schema = self.benchmark.get_active_schema(database=db_name)
            print("***", schema.database, "***")
            already_processed_tables = self.vector_search.query_vector_db(
                query="SELECT table_name FROM table_descriptions WHERE database_name = %s",
                params = [db_name]
            )
            already_processed_tables = [r[0] for r in already_processed_tables]
            for table in tqdm(schema.tables, desc=db_name):
                if table.name in already_processed_tables:
                    continue
                t_string = table.name + str(
                    [f"name: {c.name}, type: {c.data_type}" 
                     for c in table.columns
                     ])
                llm_response, token_count = self.llm.call_llm(
                    prompt=f"Write a three sentence description of the table: {t_string}. You should describe it at a conceptual level in a way that communicates all of the semantic meaning of the table. Do not refer to the object as a table or database object. Instead, describe it as the real world object that it represents. Encase the description in a json object with the key 'description'. Start the json block with ```json and end it with ```",
                    model=self.llm_model
                    )
                json_dict = self.llm.extract_json_from_response(llm_response)
                description = json_dict["description"]                
                self.vector_search.encode_table_description_into_db(
                    db_name=db_name, table_name=table.name, table_description=description
                    )
                self.vector_search.encode_table_description_sentences_into_db(
                    db_name=db_name, table_name=table.name, table_description=description
                    )
            processing_times[db_name] = time.perf_counter() - s_time
        return processing_times
                

    def get_schema_subset(
            self,
            benchmark_question: BenchmarkQuestion,
            use_tasql: bool = False
            ) -> SchemaSubsetterResult:
        table_scores, vector_search_tokens = self._do_vector_search_table_retrieval(
            benchmark_question=benchmark_question,
            distance_threshold=1.0,
            schema_proportion=0.6,
            chunk_level="whole"
        )
        table_list, table_select_tokens = self._do_llm_table_selection(
            benchmark_question=benchmark_question,
            table_scores=table_scores,
            max_tables_per_turn=800
        )
        subset_tables = []
        for table in table_list:
            try:
                subset_tables.append(benchmark_question.schema.get_table_by_name(table_name=table))
            except KeyError as e:
                subset_tables.append(SchemaTable(name=table))
        subset = Schema(database=benchmark_question.schema.database, tables=subset_tables)
        subset, column_select_tokens = self._do_llm_column_selection(
            question=benchmark_question,
            source_schema=subset,
            max_tables_per_turn=800
        )
        with open("./src/SchemaSubsetter/Skalpel/logs/subsetting_log.log", "wt") as f:
            f.write(
                f"DB: {benchmark_question.schema.database}, Q: {benchmark_question.question_number} {benchmark_question.question}"
                )
            f.write(str(subset))
            f.write(f"Tokens: {vector_search_tokens + table_select_tokens}\n\n\n")
        return SchemaSubsetterResult(
            schema_subset=subset,
            prompt_tokens=vector_search_tokens + table_select_tokens + column_select_tokens
        )

    

    def _do_tasql_schema_subsetting(
            self, 
            benchmark_question: BenchmarkQuestion,
            schema_partition_table_count: int = 500,
            do_vector_search_sort: bool = True,
            vector_search_schema_proportion=1.0
            ) -> SchemaSubsetterResult:
        final_subset: Schema = Schema(database=benchmark_question.schema.database, tables=[])
        token_count = 0
        if do_vector_search_sort:
            table_scores, vector_search_tokens = self._do_vector_search_table_retrieval(
                benchmark_question=benchmark_question,
                distance_threshold=1.0,
                schema_proportion=vector_search_schema_proportion,
                chunk_level="whole"
            )
            sorted_tables = [benchmark_question.schema.get_table_by_name(t_name[0]) for t_name in table_scores]
            benchmark_question.schema.tables = sorted_tables
        for i in range(0, len(benchmark_question.schema.tables), schema_partition_table_count):
            schema_partition = Schema(
                database=benchmark_question.schema.database,
                tables=benchmark_question.schema.tables[i:i+schema_partition_table_count]
            )
            for table in final_subset.tables:
                if not schema_partition.table_exists(table.name):
                    schema_partition.tables.append(table)
            subset_results = self.tasql.get_schema_subset(BenchmarkQuestion(
                question=benchmark_question.question,
                query=benchmark_question.query,
                question_number=benchmark_question.question_number,
                schema=schema_partition
            ))
            token_count += subset_results.prompt_tokens
            for table in subset_results.schema_subset.tables:
                if not final_subset.table_exists(table.name):
                    final_subset.tables.append(table)
        return SchemaSubsetterResult(
            schema_subset=final_subset,
            prompt_tokens=token_count
        )

        


    def _do_llm_table_selection(
            self,
            benchmark_question: BenchmarkQuestion,
            table_scores: dict[str, float],
            max_tables_per_turn: int = 50
        ) -> tuple[list, int]:
        with open("./src/SchemaSubsetter/Skalpel/prompts/select_tables_by_descriptions.prompt") as f:
            prompt = f.read()
        tables_score_sorted = [(
            table_scores[t_name], t_name, self._get_table_description(
                db_name=benchmark_question.schema.database, 
                table_name=t_name
                )
            ) for t_name in table_scores.keys()
            ]
        # tables_score_sorted.sort(key=lambda x: x[0])
        already_selected = set()     
        total_tokens = 0   
        for i in range(0, len(table_scores), max_tables_per_turn):
            tables_fragment = tables_score_sorted[i:i+max_tables_per_turn]
            table_descriptions = "\n".join([f"Table: {t[1]} Description: {t[2]}" for t in tables_fragment])
            formatted_prompt = prompt.format(
                table_count=len(table_scores),
                max_tables=max_tables_per_turn,
                already_selected_tables="\n".join(already_selected),
                query=benchmark_question.question,
                table_descriptions=table_descriptions
            )
            llm_response, select_tokens = self.llm.call_llm(formatted_prompt, model=self.llm_model)
            total_tokens += select_tokens
            table_list = self.llm.extract_json_from_response(llm_response, expected_type="dict", model=self.llm_model)
            for table in table_list.keys():
                already_selected.add(table)
        return list(already_selected), total_tokens

        

    def _do_llm_column_selection(
            self,
            question: BenchmarkQuestion,
            source_schema: Schema,
            max_tables_per_turn: int = 50
            ) -> Schema:
        with open("./src/SchemaSubsetter/Skalpel/prompts/select_columns_from_table_subsets.prompt") as f:
            prompt = f.read()
        already_selected_columns = set()
        table_ddls = [t.as_ddl(ident_enclose_l="`", ident_enclose_r="`") for t in source_schema.tables]
        subset_dict = defaultdict(set)
        total_tokens_used = 0
        for i in range(0, len(table_ddls), max_tables_per_turn):
            tables_fragment = table_ddls[i:i+max_tables_per_turn]
            prompt = prompt.format(
                max_tables=max_tables_per_turn,
                already_selected_columns=already_selected_columns,
                query=question.question,
                schema="\n".join(tables_fragment)
            )
            llm_response, select_tokens = self.llm.call_llm(prompt, model=self.llm_model)
            total_tokens_used += select_tokens
            column_list = self.llm.extract_json_from_response(llm_response, expected_type="list", model=self.llm_model)
            for column in column_list:
                already_selected_columns.add(column)
                subset_dict[".".join(column.split(".")[:-1])].add(column.split(".")[-1])
        schema_subset = Schema(database=source_schema.database, tables=[])
        for k in subset_dict:
            schema_subset.tables.append(SchemaTable(
                name=k,
                columns=[TableColumn(name=c) for c in subset_dict[k]]
            ))
        return schema_subset, total_tokens_used

    def _get_table_description(self, db_name: str, table_name: str) -> str:
        if (db_name, table_name) in self.table_description_cache.keys():
            return self.table_description_cache[(db_name, table_name)]
        else:
            return self.vector_search.get_table_description_from_db(db_name, table_name)


        
    def _do_vector_search_table_retrieval(
            self,
            benchmark_question: BenchmarkQuestion,
            distance_threshold: float,
            schema_proportion: float,
            chunk_level: str # whole | sentence
        ) -> tuple[dict[str, float], int]:
        schema_tables = {}
        question_breakdown_list, decompose_token_usage = self._decompose_question(benchmark_question)
        all_results = []
        for item in question_breakdown_list:
            if chunk_level == "sentence":
                table_descr_result = self.vector_search.get_similar_table_description_sentences_from_db(
                    db_name=benchmark_question.schema.database,
                    input_sequence=item,
                    distance_threshold=distance_threshold,
                    schema_proportion=schema_proportion
                    )
                all_results.append(table_descr_result)
            if chunk_level == "whole":
                table_descr_result = self.vector_search.get_similar_table_descriptions_from_db(
                    db_name=benchmark_question.schema.database,
                    input_sequence=item,
                    distance_threshold=distance_threshold,
                    schema_proportion=schema_proportion
                )
                all_results.append(table_descr_result)
        for table_descr_result in all_results:
            for wid in table_descr_result.tables:
                if wid.database_identifier not in schema_tables.keys():
                    schema_tables[wid.database_identifier] = wid.distance
                elif wid.distance < schema_tables[wid.database_identifier]:
                    schema_tables[wid.database_identifier] = wid.distance
        return schema_tables, decompose_token_usage



    def _decompose_question(
            self,
            benchmark_question: BenchmarkQuestion
        ) -> tuple[list[str], int]:
        question_breakdown, describe_token_usage = self._describe_objects_in_question(benchmark_question)
        question_breakdown_list = self.llm.extract_json_from_response(question_breakdown, model=self.llm_model)
        question_breakdown_list = [str(q) for q in question_breakdown_list]
        return question_breakdown_list, describe_token_usage


    def _describe_objects_in_question(
            self,
            benchmark_question: BenchmarkQuestion
        ) -> tuple[list[str], int]:
        prompt = """
Describe the various objects, concepts, etc. in a natural language query. 
Provide it as a JSON list of strings with each string describing an object in the question.
Your output must be only a well-formed JSON list.
Do not include elipses, as that will cause JSON parsing to fail.
JSON parsing cannot fail.
Failing to parse will result in termination from your position.

Example:
natural language query: How many toyota tacomas were involved in crashes where the side impact airbags deployed?

Result:
```json
[
    "A toyota tacoma is a mid-size pickup truck manufactured by Toyota.",
    "Toyota is a vehicle make.",
    "Tacoma is a model designator.",
    "To be involved in a crash is a situation where a vehicle collides with another vehicle or object, or some other unintended event that causes damage.",
    "A crash where an airbag deployed suggests a certain severity level.",
    "side impact airbags are safety features in most modern vehicles designed to protect occupants during collisions."
]
```

Describe the objects, concepts, etc. in the question: {question}
""".format(question=benchmark_question.question)
        response, token_usage = self.llm.call_llm(prompt, model=self.llm_model)
        return response, token_usage


    