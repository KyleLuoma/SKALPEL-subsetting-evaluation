from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.QueryResult import QueryResult

import json
import os
from os.path import dirname, abspath
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)


class Spider2NlSqlBenchmark(NlSqlBenchmark):

    name = "spider2"

    def __init__(self):
        super().__init__()
        self.name = Spider2NlSqlBenchmark.name
        self.benchmark_folder = dirname(dirname(dirname(dirname(abspath(__file__))))) + "/benchmarks/spider2"
        self.gold_query_instance_ids = self._get_gold_query_instance_ids()
        self.gold_query_lookup = self._make_gold_query_lookup_by_instance_id()
        self.questions_list = self._get_questions_with_instance_ids(self.gold_query_instance_ids)
        self.databases: list[str] = []
        self.database_type_lookup = self._make_database_type_lookup()
        for q in self.questions_list:
            if q["db"] not in self.databases:
                self.databases.append(q["db"])
        self.active_database_questions = self.__load_active_database_questions()
        self.active_database_queries = self.__load_active_database_queries()
        self.sql_dialect = "sqlite" #sqlite syntax should cover use cases for bigquery and snowflake as well


    def _get_gold_query_instance_ids(self) -> list:
        sql_files_path = os.path.join(self.benchmark_folder, "spider2-lite/evaluation_suite/gold/sql")
        gold_query_instance_ids = []
        
        for file_name in os.listdir(sql_files_path):
            if file_name.endswith(".sql"):
                gold_query_instance_ids.append(file_name.replace(".sql", ""))
        
        return gold_query_instance_ids
    

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


    def _get_questions_with_instance_ids(self, instance_ids) -> list[dict]:
        with open(
            os.path.join(self.benchmark_folder, "spider2-lite/spider2-lite.jsonl"), 
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
        return active_schema
    

    def set_active_schema(self, database_name) -> None:
        super().set_active_schema(database_name)


    def get_active_question(self):
        question = super().get_active_question()
        # question.query_dialect = self.database_type_lookup[question.schema.database]
        question.query_dialect = "sqlite"
        return question
    

    def execute_query(self, query, database = None, question = None) -> QueryResult:
        raise NotImplementedError


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