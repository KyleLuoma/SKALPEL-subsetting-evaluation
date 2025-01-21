from NlSqlBenchmark.QueryResult import QueryResult
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)


"""
Super class for all the benchmarks we use in the SKALPEL project to evaluate schema subsetting
"""
class NlSqlBenchmark:

    name = "abstract"

    def __init__(self):
        self.databases = ["database1"]
        self.active_database = 0
        self.active_database_questions = []
        self.active_database_queries = []
        self.active_question_no = 0
        self.db_connection = None
        self.name = "abstract"


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
        question = self.get_active_question()
        self.active_question_no += 1
        return question
    


    def __len__(self):
        return 0
        
        
    def get_active_question(self) -> BenchmarkQuestion:
        return BenchmarkQuestion(
            question=self.active_database_questions[self.active_question_no],
            query=self.active_database_queries[self.active_question_no],
            question_number=self.active_question_no,
            schema=self.get_active_schema()
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
        pass
    
    def execute_query(
            self, query: str, database: str = None, question: int = None
            ) -> QueryResult:
        if database == None:
            database = self.databases[self.active_database]
        if question == None:
            question = self.active_database_questions[self.active_question_no]
        return {
            "result_set": {},
            "database": database,
            "question": question,
            "error_message": ""
        }
    

    def get_sample_values(
            self, table_name: str, column_name: str, num_values: int = 2
        ) -> list:
        sample_values = []
        query_params = [column_name, table_name, num_values]
        query = """
SELECT ? FROM ? LIMIT ?
"""
        return sample_values

    
    def set_active_question_number(self, number: int = 0):
        self.active_question_no = number
        return self
    
    def __load_active_database_questions(self) -> list:
        return self.active_database_questions
    
    def __load_active_database_queries(self) -> list:
        return self.active_database_queries
    
    def __get_db_connection(self):
        pass

