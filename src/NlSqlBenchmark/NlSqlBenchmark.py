"""
Super class for all the benchmarks we use in the SKALPEL project to evaluate schema subsetting
"""
class NlSqlBenchmark:

    def __init__(self):
        self.databases = []
        self.active_database = 0
        self.active_database_questions = []
        self.active_database_queries = []
        self.active_question_no = 0
        self.db_connection = None


    def __iter__(self):
        return self

    def __next__(self):
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
        


    def get_active_question(self) -> dict:
        return {
            "question": self.active_database_questions[self.active_question_no],
            "query": self.active_database_queries[self.active_question_no],
            "database": self.databases[self.active_database],
            "question_number": self.active_question_no
        }
    
    def get_active_schema(self, database: str = None) -> dict:
        return {
            "tables": [
                {
                    "name": "table1",
                    "columns": [
                        {
                            "name": "column1",
                            "type": "int"
                        }
                    ],
                    "primary_keys": ["column1"],
                    "foreign_keys": [
                        {"columns": ["column1"], "references": ("table1", ["column1"])}
                    ]
                }
            ]
        }
    
    def execute_query(
            self, query: str, database: str = None, question: int = None
            ) -> dict:
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
    
    def __load_active_database_questions(self) -> list:
        return self.active_database_questions
    
    def __load_active_database_queries(self) -> list:
        return self.active_database_queries
    
    def __get_db_connection(self):
        pass

