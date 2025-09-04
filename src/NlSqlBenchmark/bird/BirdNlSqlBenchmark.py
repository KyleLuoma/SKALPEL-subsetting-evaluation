import json
import sqlite3
from os.path import dirname, abspath
import os

from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.QueryResult import QueryResult
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion

from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)

class BirdNlSqlBenchmark(NlSqlBenchmark):

    name = "bird"
    
    def __init__(self):
        super().__init__()
        self.benchmark_folder = dirname(dirname(dirname(dirname(abspath(__file__))))) + "/benchmarks/bird/dev_20240627"
        self.tables_dict = self.__load_tables_dict(self.benchmark_folder)
        self.questions_list = self.__load_questions_list()
        self.databases = [t["db_id"] for t in self.tables_dict]
        self.schema_cache = {}
        self.active_database_questions = self.__load_active_database_questions()
        self.active_database_queries = self.__load_active_database_queries()
        self.name = "bird"
        self.sql_dialect = "sqlite"

    @staticmethod
    def get_database_names() -> list:
        benchmark_folder = dirname(dirname(dirname(dirname(abspath(__file__))))) + "/benchmarks/bird/dev_20240627"
        tables_dict = BirdNlSqlBenchmark.__load_tables_dict(benchmark_folder=benchmark_folder)
        databases = [t["db_id"] for t in tables_dict]
        return databases

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
                return schema
            except FileNotFoundError as e:
                pass
        else:
            pickle_the_schema = False
        
        for s in self.tables_dict:
            if s["db_id"] == database:
                schema_dict = s
                break
        column_descriptions = self._load_database_description(db_name=database)
        active_schema = Schema(
            database=database,
            tables=[]
            )
        for i in range(0, len(schema_dict["table_names_original"])):
            # Get table names
            table_name = schema_dict["table_names_original"][i]
            active_schema.tables.append(
                SchemaTable(
                    name=table_name,
                    columns=[],
                    primary_keys=[],
                    foreign_keys=[]
                    )
                )
            columns = []
            # get column names
            for (c, t) in zip(schema_dict["column_names_original"], schema_dict["column_types"]):
                if c[0] == i:
                    column_name = c[1]
                    column_description = None
                    value_description = None
                    if f"{table_name}.{column_name}" in column_descriptions.keys():
                        column_description = column_descriptions[f"{table_name}.{column_name}"]["column_description"]
                        value_description = column_descriptions[f"{table_name}.{column_name}"]["value_description"]
                    columns.append(TableColumn(
                        name=column_name, 
                        data_type=t,
                        description=column_description,
                        value_description=value_description
                        ))
            active_schema.tables[i].columns = columns
            active_schema.tables[i].primary_keys = []
            active_schema.tables[i].foreign_keys = []
            # Get primary keys
            for composite_key in schema_dict["primary_keys"]:
                if type(composite_key) != list:
                    composite_key = [composite_key]
                composit_col_names = []
                for key in composite_key:
                    key_table_name = schema_dict["table_names_original"][
                        schema_dict["column_names_original"][key][0]
                    ]
                    if key_table_name != schema_dict["table_names_original"][i]:
                        continue
                    key_column_name = schema_dict["column_names_original"][key][1]
                    composit_col_names.append(key_column_name)
                if len(composit_col_names) > 0:
                    active_schema.tables[i].primary_keys += composit_col_names
            # Get foreign keys
            for fk_reference in schema_dict["foreign_keys"]:
                new_fk = ForeignKey(columns=[], references=None)
                fk = fk_reference[0]
                ref_pk = fk_reference[1]
                if type(ref_pk) != list:
                    ref_pk = [ref_pk]
                if type(fk) != list:
                    fk = [fk]                
                for key_column in fk:
                    key_table_name = schema_dict["table_names_original"][
                        schema_dict["column_names_original"][key_column][0]
                    ]
                    if key_table_name != schema_dict["table_names_original"][i]:
                        continue
                    new_fk.columns.append(
                        schema_dict["column_names_original"][key_column][1]
                    )
                referenced_table_name = schema_dict["table_names_original"][
                        schema_dict["column_names_original"][ref_pk[0]][0]
                    ]
                referenced_columns = []
                for key_column in ref_pk:
                        referenced_columns.append(schema_dict["column_names_original"][key_column][1])
                new_fk.references = (referenced_table_name, referenced_columns)
                if len(new_fk.columns) > 0:
                    active_schema.tables[i].foreign_keys.append(new_fk)
        self.schema_cache[database] = active_schema
        if pickle_the_schema:
            self._store_schema_pickle(active_schema)
        return active_schema
    


    def set_active_schema(self, database_name: str) -> None:
        schema_lookup = {k: v for v, k in enumerate(self.databases)}
        self.active_database = schema_lookup[database_name]
        self.active_database_questions = self.__load_active_database_questions()
        self.active_database_queries = self.__load_active_database_queries()
    


    def execute_query(self, query: str, database: str = None, question: int = None) -> QueryResult:
        if database == None:
            database = self.databases[self.active_database]
        if question == None:
            question = self.active_question_no
        con = sqlite3.connect(
            f"{self.benchmark_folder}/dev_databases/dev_databases/{database}/{database}.sqlite"
            )
        cur = con.cursor()
        try:
            res = cur.execute(query)
            con.commit()
        except sqlite3.OperationalError as e:
            con.commit()
            return QueryResult(
                result_set=None,
                database=None,
                question=None,
                error_message=str(e)
            )
        except sqlite3.ProgrammingError as e:
            con.commit()
            return QueryResult(
                result_set=None,
                database=None,
                question=None,
                error_message=str(e)
            )
        except sqlite3.Warning as e:
            con.commit()
            return QueryResult(
                result_set=None,
                database=None,
                question=None,
                error_message=f"error: sqlite3.Warning {str(e)}, query: {query}"
            )
        result_list = res.fetchall()
        if not res.description:
            return QueryResult(
                result_set={},
                database=database,
                question=question,
                error_message=None
            )
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
            database = self.databases[self.active_database]
        con = sqlite3.connect(
            f"{self.benchmark_folder}/dev_databases/dev_databases/{database}/{database}.sqlite"
            )
        cur = con.cursor()
        query = f"select `{column_name}` from `{table_name}` limit {num_values}"
        res = cur.execute(query)
        sample_values = res.fetchall()
        return [s[0] for s in sample_values]
    

    
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
    


    def _load_database_description(self, db_name: str) -> dict[str, dict[str, str]]:
        db_path = f"{self.benchmark_folder}/dev_databases/dev_databases/{db_name}/database_description"
        csv_files = [f for f in os.listdir(db_path) if f.endswith('.csv')]
        descr_lookup = {}
        for file in csv_files:
            with open(f"{db_path}/{file}", encoding="windows-1252") as f:
                table_name = file.replace(".csv", "")
                line = f.readline()
                while line != "":
                    vals = line.split(",")
                    if len(vals) < 5:
                        break
                    descr_lookup[f"{table_name}.{vals[0]}"] = {}
                    descr_lookup[f"{table_name}.{vals[0]}"]["column_description"] = vals[2]
                    descr_lookup[f"{table_name}.{vals[0]}"]["value_description"] = vals[4] if vals[4] not in ("\n", "") else None
                    line = f.readline()
        return descr_lookup


    

