from NlSqlBenchmark.QueryResult import QueryResult
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
import os
import pickle
import json
import threading
import time
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from NlSqlBenchmark import semantic_compare


"""
Super class for all the benchmarks we use in the SKALPEL project to evaluate schema subsetting
"""
class NlSqlBenchmark:

    name = "abstract"
    schema_cache_dir = "src/NlSqlBenchmark/schema_cache"

    def __init__(self):
        self.databases = ["database1"]
        self.active_database = 0
        self.active_database_questions = []
        self.active_database_queries = []
        self.active_database_question_evidence = []
        self.active_question_no = 0
        self.db_connection = None
        self.name = "abstract"
        self.sql_dialect = "mssql"
        self.naturalness = "Native"
        self.schema_cache = {}
        self.schema_pickling_disabled = False


    def __iter__(self):
        return self

    def __next__(self) -> BenchmarkQuestion:
        if self.active_question_no >= len(self.active_database_questions):
            self.active_database += 1
            if self.active_database >= len(self.databases):
                self.__init__()
                raise StopIteration
            self.active_question_no = 0
            self.active_database_questions = self.__load_active_database_questions()
            self.active_database_queries = self.__load_active_database_queries()
            self.active_database_question_evidence = self.__load_active_database_evidences()
        question = self.get_active_question()
        self.active_question_no += 1
        return question
    


    def __len__(self):
        return 0
    

    @staticmethod
    def get_database_names() -> list:
        return ["database1"]
        
    def get_active_question(self) -> BenchmarkQuestion:
        return BenchmarkQuestion(
            question=self.active_database_questions[self.active_question_no],
            query=self.active_database_queries[self.active_question_no],
            evidence=self.active_database_question_evidence[self.active_question_no],
            query_dialect=self.sql_dialect,
            question_number=self.active_question_no,
            schema=self.get_active_schema(),
            schema_naturalness=self.naturalness,
        )

    
    def get_active_schema(self, database: str = None) -> Schema:

        return Schema(
            database=self.databases[self.active_database],
            tables=[
                SchemaTable(
                    name="table1",
                    columns=[
                        TableColumn(
                            name="column1",
                            data_type="int"
                        )
                    ],
                    primary_keys=["column1"],
                    foreign_keys=[
                        ForeignKey(
                            columns=["column1"],
                            references=("table1", ["column1"])
                        )
                    ]
                )
            ]
        )

        # return {
        #     "database": self.databases[self.active_database],
        #     "tables": [
        #         {
        #             "name": "table1",
        #             "columns": [
        #                 {
        #                     "name": "column1",
        #                     "type": "int"
        #                 }
        #             ],
        #             "primary_keys": ["column1"],
        #             "foreign_keys": [
        #                 {"columns": ["column1"], "references": ("table1", ["column1"])}
        #             ]
        #         }
        #     ]
        # }
    
    def set_active_schema(self, database_name: str) -> None:
        schema_lookup = {k: v for v, k in enumerate(self.databases)}
        self.active_database = schema_lookup[database_name]
        self.active_database_questions = self.__load_active_database_questions()
        self.active_database_queries = self.__load_active_database_queries()
        self.active_database_question_evidence = self.__load_active_database_evidences()
    
    def execute_query(
            self, query: str, database: str = None, question: int = None, query_timeout: int = None
            ) -> QueryResult:
        if database == None:
            database = self.databases[self.active_database]
        if question == None:
            question = self.active_database_questions[self.active_question_no]
        return QueryResult(
            result_set={},
            database=database,
            question=question,
            error_message=""
        )
    

    def compare_gold_to_generated_query(
            self, 
            benchmark_question: BenchmarkQuestion, 
            generated_query: str
            ) -> tuple[dict[bool, str], QueryResult, QueryResult]:
        g_st = time.perf_counter()
        gold_results = self.execute_query(
            query=benchmark_question.query,
            database=benchmark_question.schema.database
            )
        g_et = time.perf_counter()

        gen_timeout = int((g_et - g_st) * 2) + 1
        gen_timeout = min(30, gen_timeout)
        generated_results = self.execute_query(
            query=generated_query,
            database=benchmark_question.schema.database,
            query_timeout=gen_timeout
        )

        subset_check_result = semantic_compare.compare_gold_to_generated(
            gold_result=gold_results, generated_result=generated_results
        )
        return subset_check_result, gold_results, generated_results
      


    def get_sample_values(
            self, table_name: str, column_name: str, num_values: int = 2
        ) -> list:
        sample_values = []
        query_params = [column_name, table_name, num_values]
        query = """
SELECT DISTINCT ? FROM ? LIMIT ?
"""
        return sample_values
    
    def get_unique_values(
            self, 
            table_name: str, 
            column_name: str, 
            database: str = None
            ) -> set[str]:
        return set()
    

    def save_stats_to_disk(self, save_path: str):
        db_stats = {}
        for database in self.databases:
            schema = self.get_active_schema(database=database)
            total_rows = 0
            total_vals = 0
            for table in schema.tables:
                if " " in table.name and self.sql_dialect == "sqlite":
                    table_name = f"`{table.name}`"
                elif " " in table.name and self.sql_dialect == "mssql":
                    table_name = f"[{table.name}]"
                else:
                    table_name = table.name
                result = self.execute_query(
                    query=f"select count(*) as col_count from {table_name}",
                    database=database
                )
                try:
                    result.result_set = {k.lower(): result.result_set[k] for k in result.result_set.keys()}
                    row_count = result.result_set["col_count"][0]
                    total_rows += row_count
                    total_vals += (row_count * len(table.columns))
                except TypeError:
                    print(result.error_message)
                except KeyError:
                    print(result.error_message)
                    print(result.result_set)
                except AttributeError:
                    print(result.error_message)
            db_stats[database] = {
                "table_count": schema.get_table_count(),
                "column_count": schema.get_column_count(),
                "total_rows": total_rows,
                "total_vals": total_vals
            }
        with open(f"{save_path}/{self.name}_schema_stats.json", "wt") as f:
            f.write(json.dumps(db_stats, indent=4))

    
    def set_active_question_number(self, number: int = 0):
        self.active_question_no = number
        return self
    
    def __load_active_database_questions(self) -> list:
        return self.active_database_questions
    
    def __load_active_database_queries(self) -> list:
        return self.active_database_queries
    
    def __load_active_database_evidences(self) -> list[str]:
        return self.active_database_question_evidence

    def __get_db_connection(self):
        pass

    @staticmethod
    def _retrieve_schema_pickle(database_name: str) -> Schema:
        schema_path = os.path.join(
            NlSqlBenchmark.schema_cache_dir, 
            f"{database_name}-Schema.pkl"
            )
        if os.path.exists(schema_path):
            with open(schema_path, "rb") as schema_file:
                schema: Schema = pickle.load(schema_file)
                return schema
        else:
            raise FileNotFoundError(f"Schema file not found at {schema_path}")


    @staticmethod
    def _store_schema_pickle(schema: Schema, alternate_name: str = None):
        if not alternate_name:
            db_name = schema.database
        else:
            db_name = alternate_name
        filename = f"{db_name}-Schema.pkl"
        schema_path = os.path.join(NlSqlBenchmark.schema_cache_dir, filename)
        os.makedirs(NlSqlBenchmark.schema_cache_dir, exist_ok=True)
        with open(schema_path, "wb") as schema_file:
            pickle.dump(schema, schema_file)
