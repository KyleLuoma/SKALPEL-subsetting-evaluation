import json
import sqlite3
from os.path import dirname, abspath
import os

from pathlib import Path

from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.QueryResult import QueryResult
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion

from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)

class BigBirdNlSqlBenchmark(NlSqlBenchmark):

    name = "bigbird"

    databases = [
        "california_schools_big",
        "card_games_big",
        "codebase_community_big",
        "debit_card_specializing_big",
        "european_football_2_big",
        "financial_big",
        "formula_1_big",
        "student_club_big",
        "superhero_big",
        "thrombosis_prediction_big",
        "toxicology_big"
    ]
    
    def __init__(self):
        super().__init__()
        self.benchmark_folder = Path(__file__).parent.parent.parent.parent / "benchmarks" / "bigbird"
        self.questions_list = self.__load_questions_list()
        self.databases = BigBirdNlSqlBenchmark.databases
        self.schema_cache = {}
        self.active_database_questions = self.__load_active_database_questions()
        self.active_database_queries = self.__load_active_database_queries()
        self.active_database_question_evidence = ["" for q in self.active_database_questions]
        self.name = BigBirdNlSqlBenchmark.name
        self.sql_dialect = "sqlite"
        self.active_database_name = self.databases[self.active_database]
        self.db_con = sqlite3.connect(f"{self.benchmark_folder}/bigbird_databases/{self.active_database_name}.sqlite")
        self.db_cur = self.db_con.cursor()

    @staticmethod
    def get_database_names() -> list:
        return BigBirdNlSqlBenchmark.databases

    def __iter__(self):
        return self

    def __next__(self) -> BenchmarkQuestion:
        if self.active_question_no >= len(self.active_database_questions):
            self.active_database += 1
            self.active_question_no = 0
            if self.active_database >= len(self.databases):
                self.__init__()
                raise StopIteration
            self.active_database_questions = self.__load_active_database_questions()
            self.active_database_queries = self.__load_active_database_queries()
            self.active_database_question_evidence = ["" for q in self.active_database_questions]
        question = self.get_active_question()
        self.active_question_no += 1
        return question
    


    def __len__(self):
        return len(self.questions_list)



    def get_active_question(self) -> BenchmarkQuestion:
        return super().get_active_question()



    def get_active_schema(self, database: str = None) -> Schema:
        if database == None:
            database = self.databases[self.active_database]
        if database in self.schema_cache.keys():
            return self.schema_cache[database]
        
        pickle_the_schema = True
        if not self.schema_pickling_disabled:
            try:
                schema = self._retrieve_schema_pickle(database_name=database)
                self.schema_cache[database] = schema
                for table in schema.tables:
                    if table.foreign_keys == None:
                        table.foreign_keys = []
                    else:
                        for fk in table.foreign_keys:
                            if type(fk.references[1]) == str:
                                fk.references = (fk.references[0], [fk.references[1]])
                return schema
            except FileNotFoundError as e:
                pass
        else:
            pickle_the_schema = False
        
        active_schema = Schema(
            database=database,
            tables=[]
            )
        
        q_result = self.execute_query(
            query="SELECT name FROM sqlite_master WHERE type='table';",
            database=database
        )

        tables = q_result.result_set["name"]

        for table_name in tables:
            new_table = SchemaTable(
                name=table_name
            )
            q_result = self.execute_query(
                query=f"SELECT name AS column_name, type AS data_type FROM pragma_table_info('{table_name}')",
                database=database
            )
            for i in range(0, len(q_result.result_set["column_name"])):
                c_name = q_result.result_set["column_name"][i]
                c_type = q_result.result_set["data_type"][i]
                sample_result = self.get_sample_values(
                    table_name=table_name,
                    column_name=c_name,
                    database=database
                )
                unique_result = self.get_unique_values(
                    table_name=table_name,
                    column_name=c_name,
                    database=database
                )
                new_table.columns.append(TableColumn(
                    name=c_name,
                    data_type=c_type,
                    sample_values=sample_result,
                    unique_values=unique_result
                ))
            q_result = self.execute_query(
                query=f"SELECT name FROM pragma_table_info('{table_name}') WHERE pk == 1;",
                database=database
            )
            for pk in q_result.result_set["name"]:
                new_table.primary_keys.append(pk)
            q_result = self.execute_query(
                query=f"SELECT \"from\", \"table\", \"to\" FROM pragma_foreign_key_list('{table_name}');"
            )
            new_table.foreign_keys = []
            for i in range(0, len(q_result.result_set["from"])):
                fks = []
                fks.append(ForeignKey(
                    columns=[q_result.result_set["from"][i]],
                    references=(q_result.result_set["table"][i], [q_result.result_set["to"][i]])
                ))
                new_table.foreign_keys = fks
            active_schema.tables.append(new_table)
            
        self.schema_cache[database] = active_schema
        if pickle_the_schema:
            self._store_schema_pickle(active_schema)
        return active_schema
    


    def set_active_schema(self, database_name: str) -> None:
        schema_lookup = {k: v for v, k in enumerate(self.databases)}
        self.active_database = schema_lookup[database_name]
        self.active_database_name = database_name
        self.active_database_questions = self.__load_active_database_questions()
        self.active_database_queries = self.__load_active_database_queries()
        self.active_database_question_evidence = ["" for q in self.active_database_questions]
        self.db_cur.close()
        self.db_con.close()
        self.db_con = sqlite3.connect(f"{self.benchmark_folder}/bigbird_databases/{self.active_database_name}.sqlite")
        self.db_cur = self.db_con.cursor()
    


    def execute_query(self, query: str, database: str | None= None, question: int | None = None, query_timeout: int | None = None) -> QueryResult:
        if database == None:
            database = self.databases[self.active_database]
        if question == None:
            question = self.active_question_no
        if query == "":
            return QueryResult(
                result_set=None,
                database=None,
                question=None,
                error_message="query argument was an empty string."
            )
        query = query.split(";")[0]
        if database != self.active_database_name:
            con = sqlite3.connect(
                f"{self.benchmark_folder}/bigbird_databases/{database}.sqlite"
                )
            cur = con.cursor()
        else:
            con = self.db_con
            cur = self.db_cur
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
            question=question,
            error_message=None
        )
    

    def get_sample_values(self,  table_name: str, column_name: str, database: str = None, num_values: int = 2) -> list[str]:
        if database == None:
            database = self.active_database
        query = f"select `{column_name}` from `{table_name}` limit {num_values}"
        q_result = self.execute_query(
            query=query,
            database=database
        )
        if q_result.result_set == None:
            return []
        return q_result.result_set[column_name]
    

    
    # Adapted from CHESS Logic:
    # https://github.com/ShayanTalaei/CHESS
    def get_unique_values(
            self, 
            table_name: str, 
            column_name: str, 
            database: str = None
            ) -> set[str]:
        if any(
            keyword in column_name.lower() 
            for keyword in [
                "_id", " id", "url", "email", "web", "time", "phone", "date", "address"
            ]) or column_name.endswith("Id"):
            return set()
        result = self.execute_query(
            query=f"""
                    SELECT SUM(LENGTH(unique_values)) as val_sum, COUNT(unique_values) as val_count
                    FROM (
                        SELECT DISTINCT `{column_name}` AS unique_values
                        FROM `{table_name}`
                        WHERE `{column_name}` IS NOT NULL
                    ) AS subquery
                """,
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
            try:
                result = self.execute_query(
                        query = f"SELECT DISTINCT `{column_name}` FROM `{table_name}` WHERE `{column_name}` IS NOT NULL",
                        database=database
                        )
                values = set(result.result_set[column_name])
            except:
                values = set()
        return values

        
        


    @staticmethod
    def __load_tables_dict(benchmark_folder: str) -> dict:
        with open(f"{benchmark_folder}/dev_tables.json") as f:
            dev_tables = json.load(f)
        return dev_tables
    

    def __load_questions_list(self) -> list:
        with open(f"{self.benchmark_folder}/dev.json") as f:
            dev_questions = json.load(f)
        return dev_questions
    

    def __load_active_database_questions(self) -> list[dict]:
        questions = []
        for q in self.questions_list:
            if q["db_id"] == self.databases[self.active_database]:
                questions.append(q["question"])
        return questions
    

    def __load_active_database_queries(self) -> list[str]:
        queries = []
        for q in self.questions_list:
            if q["db_id"] == self.databases[self.active_database]:
                queries.append(q["SQL"])
        return queries
    





    

