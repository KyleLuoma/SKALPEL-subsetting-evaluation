from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.QueryResult import QueryResult

import time
import json
import os
import pickle

from os.path import dirname, abspath
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)

import snowflake.connector
import sqlite3
from google.cloud import bigquery
import pandas as pd

class Spider2NlSqlBenchmark(NlSqlBenchmark):

    name = "spider2"

    exclude_from_eval = [
        "bq032.sql", # Contains * EXCEPT clause
        "bq119.sql", # Contains * EXCEPT clause
        "bq098.sql"  # Unable to parse, reason unknown
    ]   

    def __init__(self):
        super().__init__()
        self.name = Spider2NlSqlBenchmark.name
        self.benchmark_folder = dirname(dirname(dirname(dirname(abspath(__file__))))) + "/benchmarks/spider2"
        self.gold_query_instance_ids = self._get_gold_query_instance_ids(self.benchmark_folder)
        self.gold_query_lookup = self._make_gold_query_lookup_by_instance_id()
        self.questions_list = self._get_questions_with_instance_ids(self.benchmark_folder, self.gold_query_instance_ids)
        self.instance_id_lookup = self._make_instance_id_lookup(self.questions_list)
        self.databases: list[str] = []
        self.database_type_lookup = self._make_database_type_lookup()
        for q in self.questions_list:
            if q["db"] not in self.databases:
                self.databases.append(q["db"])
        self.active_database_questions = self.__load_active_database_questions()
        self.active_database_queries = self.__load_active_database_queries()
        self.sql_dialect = "sqlite" #sqlite syntax should cover use cases for bigquery and snowflake as well
        self.schema_cache = {}


    @staticmethod
    def get_database_names() -> list:
        benchmark_folder = dirname(dirname(dirname(dirname(abspath(__file__))))) + "/benchmarks/spider2"
        instance_ids = Spider2NlSqlBenchmark._get_gold_query_instance_ids(
            benchmark_folder=benchmark_folder
        )
        question_list = Spider2NlSqlBenchmark._get_questions_with_instance_ids(
            benchmark_folder=benchmark_folder,
            instance_ids=instance_ids
        )
        databases = []
        for q in question_list:
            if q["db"] not in databases:
                databases.append(q["db"])
        return databases


    @staticmethod
    def _get_gold_query_instance_ids(benchmark_folder: str) -> list:
        sql_files_path = os.path.join(benchmark_folder, "spider2-lite/evaluation_suite/gold/sql")
        gold_query_instance_ids = []
        
        for file_name in os.listdir(sql_files_path):
            if file_name.endswith(".sql") and file_name not in Spider2NlSqlBenchmark.exclude_from_eval:
                gold_query_instance_ids.append(file_name.replace(".sql", ""))
        
        return gold_query_instance_ids
    

    @staticmethod
    def _get_questions_with_instance_ids(benchmark_folder: str, instance_ids: list) -> list[dict]:
        with open(
            os.path.join(benchmark_folder, "spider2-lite/spider2-lite.jsonl"), 
            "r",
            encoding='utf-8'
            ) as f:
            instance_list = []
            for line in f:
                instance_list.append(json.loads(line))
        questions = []
        for inst_dict in instance_list:
            if inst_dict["instance_id"] in instance_ids:
                questions.append(inst_dict)
        return questions
    

    def _make_gold_query_lookup_by_instance_id(self) -> dict:
        sql_files_path = os.path.join(self.benchmark_folder, "spider2-lite/evaluation_suite/gold/sql")
        lookup = {}
        for file_name in os.listdir(sql_files_path):
            if file_name.endswith(".sql"):
                with open(
                    os.path.join(self.benchmark_folder, "spider2-lite/evaluation_suite/gold/sql", file_name),
                    encoding='utf-8'
                    ) as f:
                    lookup[(file_name.replace(".sql", ""))] = f.read()
        return lookup


    def _make_instance_id_lookup(self, questions_list: list[dict]) -> dict[tuple[str, int] : str]:
        lookup_dict = {}
        question_counter = {}
        for q in questions_list:
            if q["db"] not in question_counter.keys():
                question_counter[q["db"]] = 0
            else:
                question_counter[q["db"]] += 1
            lookup_dict[(q["db"], question_counter[q["db"]])] = q["instance_id"]
        return lookup_dict


    def _make_database_type_lookup(self) -> dict:
        database_path = os.path.join(self.benchmark_folder, "spider2-lite/resource/databases")
        bigquery_databases = os.listdir(os.path.join(database_path, "bigquery"))
        lookup_dict = {}
        for dir in bigquery_databases:
            lookup_dict[dir] = "bigquery"
        snowflake_databases = os.listdir(os.path.join(database_path, "snowflake"))
        for dir in snowflake_databases:
            lookup_dict[dir] = "snowflake"
        local_databases = os.listdir(os.path.join(database_path, "sqlite"))
        for dir in local_databases:
            lookup_dict[dir] = "sqlite"
        return lookup_dict
    

    def _make_bigquery_schema_lookup(self) -> dict[str,list]:
        lookup = {}
        for q in self.questions_list:
            if self.database_type_lookup[q["db"]] == "bigquery":
                bq_dbs = set()
                query_lines = self.gold_query_lookup[q["instance_id"]].split("\n")
                query_lines = [l for l in query_lines if "bigquery-public-data." in l]
                for l in query_lines:
                    db_name = l.split(".")[1]
                    bq_dbs.add(db_name)
                if q["db"] not in lookup.keys():
                    lookup[q["db"]] = bq_dbs
                else:
                    lookup[q["db"]] = lookup[q["db"]].union(bq_dbs)
        return lookup


    def __iter__(self):
        return self
    

    def __next__(self):
        if self.active_question_no >= len(self.active_database_questions):
            self.active_database += 1
            self.active_question_no = 0
            if self.active_database >= len(self.databases):
                self.__init__()
                raise StopIteration
            self.active_database_questions = self.__load_active_database_questions()
            self.active_database_queries = self.__load_active_database_queries()
        question = self.get_active_question()
        self.active_question_no += 1
        return question
    

    def __len__(self):
        return len(self.questions_list)
    

    def __load_active_database_questions(self):
        questions = []
        for q in self.questions_list:
            if q["db"] == self.databases[self.active_database]:
                questions.append(q["question"])
        return questions
    

    def __load_active_database_queries(self):
        queries = []
        for q in self.questions_list:
            if q["db"] == self.databases[self.active_database]:
                queries.append(self.gold_query_lookup[q["instance_id"]])
        return queries


    def get_active_schema(self, database = None) -> Schema:
        if database == None:
            database = self.databases[self.active_database]
        if database in self.schema_cache.keys():
            return self.schema_cache[database]
        pickle_the_schema = True
        if not self.schema_pickling_disabled:
            pickle_db_name = f"{database}-{self.database_type_lookup[database]}"
            try:
                schema = self._retrieve_schema_pickle(database_name=pickle_db_name)
                self.schema_cache[database] = schema
                return schema
            except FileNotFoundError as e:
                pass
        else:
            pickle_the_schema = False
        active_schema = Schema(
            database=database,
            tables=[]
        )
        db_dir = os.path.join(
            self.benchmark_folder, 
            "spider2-lite/resource/databases", 
            self.database_type_lookup[database], 
            database
            )
        schema_folders = [filename for filename in os.listdir(db_dir)]
        for schema_folder in schema_folders:
            if self.database_type_lookup[database] == "sqlite":
                schema_folder = ""
            table_files = [
                filename for filename in os.listdir(os.path.join(db_dir, schema_folder)) if ".json" in filename
                ]
            for table_file in table_files:
                table_dict = json.loads(open(
                    os.path.join(db_dir, schema_folder, table_file), 
                    "r",
                    encoding="utf-8"
                    ).read())
                new_table = SchemaTable(
                    name=table_dict["table_fullname"],
                    columns=[],
                    primary_keys=[],
                    foreign_keys=[]
                )
                nested_insert = ""
                if "nested_column_names" in table_dict.keys():
                    nested_insert = "nested_"
                sample_values = {}
                if "sample_rows" in table_dict.keys():
                    for row_dict in table_dict["sample_rows"]:
                        for col_name in row_dict:
                            if col_name not in sample_values.keys():
                                sample_values[col_name] = []
                            sample_values[col_name].append(row_dict[col_name])

                for ix, column_name in enumerate(table_dict[f"{nested_insert}column_names"]):
                    col_sample_values = None
                    if column_name in sample_values.keys():
                        col_sample_values = sample_values[column_name]
                    new_table.columns.append(TableColumn(
                        name=column_name,
                        data_type=table_dict[f"{nested_insert}column_types"][ix],
                        description=table_dict["description"][ix],
                        sample_values=col_sample_values
                    ))
                active_schema.tables.append(new_table)
            if self.database_type_lookup[database] == "sqlite":
                break
        self.schema_cache[database] = active_schema
        if pickle_the_schema:
            self._store_schema_pickle(active_schema, pickle_db_name)
        return active_schema
    

    def set_active_schema(self, database_name: str) -> None:
        schema_lookup = {k: v for v, k in enumerate(self.databases)}
        self.active_database = schema_lookup[database_name]
        self.active_database_questions = self.__load_active_database_questions()
        self.active_database_queries = self.__load_active_database_queries()


    def get_active_question(self):
        question = super().get_active_question()
        question.query_dialect = self.database_type_lookup[question.schema.database]
        question.query_filename = self.instance_id_lookup[
            (question.schema.database, question.question_number)
            ] + ".sql"
        return question
    

    def execute_query(
            self, 
            query: str, 
            database: str = None, 
            question: str = None, 
            use_result_caching: bool = True, 
            simulate_exec_time_with_cache: bool = True
            ) -> QueryResult:
        if database == None:
            database = self.databases[self.active_database]
        if question == None:
            question = self.active_question_no
        dialect = self.database_type_lookup[database]

        if dialect == "sqlite":
            result = self.query_sqlite(query=query, database=database)
        elif dialect == "bigquery":
            result = self.query_bigquery(query=query, database=database, use_result_caching=use_result_caching)
        elif dialect == "snowflake":
            result = self.query_snowflake(query=query, database=database, use_result_caching=use_result_caching)
        result.question = question
        return result
    
    def query_sqlite(self, query: str, database: str) -> QueryResult:
        #benchmarks\spider2\spider2-lite\resource\databases\sqlite\local_sqlite\AdventureWorks.sqlite
        conn = sqlite3.connect(
            f"{self.benchmark_folder}/spider2-lite/resource/databases/sqlite/local_sqlite/{database}.sqlite"
            )
        cur = conn.cursor()
        try:
            res = cur.execute(query)
        except sqlite3.OperationalError as e:
            return QueryResult(
                result_set=None,
                database=None,
                question=None,
                error_message=str(e)
            )
        result_list = res.fetchall()
        columns = [d[0] for d in res.description]
        result_set_dict = {}
        for i, c in enumerate(columns):
            values = [t[i] for t in result_list]
            result_set_dict[c] = values
        return QueryResult(
            result_set=result_set_dict,
            database=database,
            question=None,
            error_message=None
        )
    


    def _cache_query_result(self, query: str, database: str, query_result: QueryResult, exec_time: int) -> int:
        result_to_cache = {"time": exec_time, "query_result": query_result}
        query_hash = hash(query)
        filename = f"{database}-{query_hash}.pkl"
        cache_dir = "./benchmarks/spider2/spider2-lite/query_result_cache"
        os.makedirs(cache_dir, exist_ok=True)
        cache_file_path = os.path.join(cache_dir, filename)
        with open(cache_file_path, "wb") as cache_file:
            pickle.dump(result_to_cache, cache_file)
        return query_hash


    def _get_cached_result(self, query: str, database: str, simulate_exec_time: bool = True) -> QueryResult:
        query_hash = hash(query)
        filename = f"{database}-{query_hash}.pkl"
        cache_dir = "./benchmarks/spider2/spider2-lite/query_result_cache"
        cache_file_path = os.path.join(cache_dir, filename)
        if os.path.exists(cache_file_path):
            with open(cache_file_path, "rb") as cache_file:
                cached_result = pickle.load(cache_file)
            if simulate_exec_time:
                time.sleep(cached_result["time"])
            return cached_result["query_result"]
        return None


    def query_bigquery(
            self, query: str, 
            database: str, 
            use_result_caching: bool = True,
            simulate_exec_time_on_cache_retrieval: bool = True
            ) -> QueryResult:
        if use_result_caching:
            cached_result = self._get_cached_result(query, database, simulate_exec_time=simulate_exec_time_on_cache_retrieval)
            if cached_result != None:
                return cached_result
        s_time = time.perf_counter()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./.local/nl-to-sql-model-eval-80a8e87ec156.json"
        client = bigquery.Client()
        try:
            query_job = client.query(query)
            results: pd.DataFrame = query_job.to_dataframe()
            query_result = QueryResult(
                result_set=results.to_dict(orient="list"),
                database=database,
                question=None
            )
        except Exception as e:
            query_result = QueryResult(
                result_set=None, database=database, question=None, error_message=str(e)
            )
        e_time = time.perf_counter()
        exec_time = e_time - s_time
        self._cache_query_result(query, database, query_result, exec_time)
        return query_result

    

    def query_snowflake(
            self, 
            query: str, 
            database: str, 
            use_result_caching: bool = True,
            simulate_exec_time_on_cache_retrieval: bool = True
            ) -> QueryResult:
        if use_result_caching:
            cached_result = self._get_cached_result(query, database, simulate_exec_time=simulate_exec_time_on_cache_retrieval)
            if cached_result != None:
                return cached_result
        s_time = time.perf_counter()
        snowflake_credential = json.load(open('./.local/snowflake_credential.json'))
        conn = snowflake.connector.connect(
            **snowflake_credential
        )
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            result_set = {c: [] for c in columns}
            for row in cursor.fetchall():
                for ix, c in enumerate(columns):
                    result_set[c].append(row[ix])
            query_result = QueryResult(result_set=result_set, database=database, question=None)
        except Exception as e:
            query_result = QueryResult(result_set=None, database=database, question=None, error_message=str(e))
        e_time = time.perf_counter()
        exec_time = e_time - s_time
        self._cache_query_result(query, database, query_result, exec_time)
        return query_result
    

    def get_sample_values(self, table_name: str, column_name: str, database: str = None, num_values: int = 2) -> list[str]:
        if database == None:
            schema = self.get_active_schema()
        else:
            schema = self.get_active_schema(database=database)
        
        table_object = None
        for table in schema.tables:
            if table_name == table.name:
                table_object = table
        if table_object == None:
            return []
        for column in table_object.columns:
            if column.name == column_name and column.sample_values != None:
                return column.sample_values[:min(len(column.sample_values) - 1, num_values)]
        return []
    

    def get_unique_values(
            self, 
            table_name: str, 
            column_name: str, 
            database: str
            ) -> set[str]:
        if any(
            keyword in column_name.lower() 
            for keyword in [
                "_id", " id", "url", "email", "web", "time", "phone", "date", "address"
            ]) or column_name.endswith("Id"):
            return set()
        ident_encase = {
            "snowflake": '"',
            "sqlite": '"',
            "bigquery": "`" 
        }
        db_type = self.database_type_lookup[database]
        query = f"""
        SELECT SUM(LENGTH(unique_values)) as val_sum, COUNT(unique_values) as val_count
        FROM (
            SELECT DISTINCT `{column_name}` AS unique_values
            FROM `{table_name}`
            WHERE `{column_name}` IS NOT NULL
        ) AS subquery
        """.replace("`", ident_encase[db_type])
        result = self.execute_query(
            query=query,
            database=database
        )
        if result.result_set == None:
            return set()
        sum_of_lengths = result.result_set["val_sum"][0] 
        count_distinct = result.result_set["val_count"][0]
        if sum_of_lengths is None or count_distinct == 0:
            return set()
        average_length = sum_of_lengths / count_distinct
        values = set()
        if (
            ("name" in column_name.lower() and sum_of_lengths < 5000000) 
            or (sum_of_lengths < 2000000 and average_length < 25) 
            or count_distinct < 100
            ):
            query = f"SELECT DISTINCT `{column_name}` FROM `{table_name}` WHERE `{column_name}` IS NOT NULL"
            query = query.replace("`", ident_encase[db_type])
            try:
                result = self.execute_query(
                        query=query,
                        database=database
                        )
                values = set(result.result_set[column_name])
            except:
                values = set()
        return values
