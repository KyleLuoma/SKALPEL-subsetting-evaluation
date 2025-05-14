import json
import sqlite3
from os.path import dirname, abspath

from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.QueryResult import QueryResult
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)

class SpiderNlSqlBenchmark(NlSqlBenchmark):

    name = "spider"


    def __init__(self):
        super().__init__()
        self.benchmark_folder = dirname(dirname(dirname(dirname(abspath(__file__))))) + "/benchmarks/spider1"
        self.name = "spider"
        self.sql_dialect = "sqlite"
        self.questions_list = self.__load_questions_list()
        self.databases: list[str] = []
        for q in self.questions_list:
            if q["db_id"] not in self.databases:
                self.databases.append(q["db_id"])
        self.tables_dict = self.__load_tables_dict()
        self.active_database_questions = self.__load_active_database_questions()
        self.active_database_queries = self.__load_active_database_queries()



    def __iter__(self):
        return self
    

    def __next__(self):
        if self.active_question_no >= len(self.active_database_questions):
            self.active_database += 1
            self.active_question_no = 0
            if self.active_database >= len(self.databases):
                self.__init__()
                self.active_database = 0
                self.active_question_no = 0
                raise StopIteration
            self.active_database_questions = self.__load_active_database_questions()
            self.active_database_queries = self.__load_active_database_queries()
        question = self.get_active_question()
        self.active_question_no += 1
        return question


    def get_active_schema(self, database = None) -> Schema:
        if database == None:
            database = self.databases[self.active_database]
        active_schema = Schema(
            database=database,
            tables=[]
        )
        for s in self.tables_dict:
            if s["db_id"] == database:
                schema_dict = s
                break
        for i in range(0, len(schema_dict["table_names_original"])):
            active_schema.tables.append(
                SchemaTable(
                    name=schema_dict["table_names_original"][i],
                    columns=[],
                    primary_keys=[],
                    foreign_keys=[]
                ))
            columns = []
            for (c, t) in zip(schema_dict["column_names_original"], schema_dict["column_types"]):
                if c[0] == i:
                    columns.append(TableColumn(name=c[1], data_type=t))
            active_schema.tables[i].columns = columns
            for key in schema_dict["primary_keys"]:
                if schema_dict["column_names_original"][key][0] == i:
                    active_schema.tables[i].primary_keys.append(schema_dict["column_names_original"][key][1])
            for fk in schema_dict["foreign_keys"]:
                if schema_dict["column_names_original"][fk[0]][0] != i:
                    continue
                new_fk = ForeignKey(
                    columns=[schema_dict["column_names_original"][fk[0]][1]],
                    references=[schema_dict["column_names_original"][fk[1]][1]]
                )
                active_schema.tables[i].foreign_keys.append(new_fk)
        return active_schema


    def set_active_schema(self, database_name) -> None:
       super().set_active_schema(database_name)


    def execute_query(self, query: str, database: str = None, question: int = None) -> QueryResult:
        # raise NotImplementedError
        if database == None:
            database = self.databases[self.active_database]
        if question == None:
            question = self.active_question_no
        con = sqlite3.connect(
            f"{self.benchmark_folder}/db/{database}.sqlite"
            )
        cur = con.cursor()
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
    

    def get_sample_values(self,  table_name: str, column_name: str, database: str = None, num_values: int = 2) -> list:
        if database == None:
            database = self.active_database
        con = sqlite3.connect(
            f"{self.benchmark_folder}/db/{database}.sqlite"
            )
        cur = con.cursor()
        query = f"select `{column_name}` from `{table_name}` limit {num_values}"
        res = cur.execute(query)
        sample_values = res.fetchall()
        return [s[0] for s in sample_values]


    def __load_questions_list(self) -> list[dict]:
        with open(f"{self.benchmark_folder}/dev.json") as f:
            dev_questions = json.load(f)
        return dev_questions


    def __load_tables_dict(self, ) -> dict:
        with open(f"{self.benchmark_folder}/tables.json") as f:
            all_tables = json.load(f)
        dev_tables = []
        for table in all_tables:
            if table["db_id"] in self.databases:
                dev_tables.append(table)
        return dev_tables
    

    def __load_active_database_questions(self) -> list[dict]:
        questions = []
        for q in self.questions_list:
            if q["db_id"] == self.databases[self.active_database]:
                questions.append(q["question"])
        return questions
    

    def __load_active_database_queries(self):
        queries = []
        for q in self.questions_list:
            if q["db_id"] == self.databases[self.active_database]:
                queries.append(q["query"])
        return queries