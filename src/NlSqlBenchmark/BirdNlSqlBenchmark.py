from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
import json
from os.path import dirname, abspath

class BirdNlSqlBenchmark(NlSqlBenchmark):
    
    def __init__(self):
        super().__init__()
        self.tables_dict = self.__load_tables_dict()
        self.questions_dict = self.__load_questions_dict()
        self.databases = [t["db_id"] for t in self.tables_dict]
        self.active_database_questions = self.__load_active_database_questions()


    def get_active_question(self) -> dict:
        return {
            "question": self.active_database_questions[self.active_question_no],
            "database": self.databases[self.active_database],
            "question_number": self.active_question_no
        }
    


    def __iter__(self):
        return self

    def __next__(self):
        if self.active_question_no >= len(self.active_database_questions):
            self.active_database += 1
            if self.active_database >= len(self.databases):
                raise StopIteration
            self.active_question_no = 0
            self.active_database_questions = self.__load_active_database_questions()
        question = self.get_active_question()
        self.active_question_no += 1
        return question



    def get_active_schema(self, database: str = None) -> dict:
        if database == None:
            database = self.databases[self.active_database]
        for s in self.tables_dict:
            if s["db_id"] == database:
                schema_dict = s
                break
        active_schema = {"tables": []}
        for i in range(0, len(schema_dict["table_names_original"])):
            active_schema["tables"].append({"name": schema_dict["table_names_original"][i]})
            columns = []
            for (c, t) in zip(schema_dict["column_names_original"], schema_dict["column_types"]):
                if c[0] == i:
                    columns.append({"name": c[1], "type": t})
            active_schema["tables"][i]["columns"] = columns
        return active_schema
    


    def __load_tables_dict(self) -> dict:
        parent_dir = dirname(dirname(dirname(abspath(__file__))))
        with open(f"{parent_dir}/benchmarks/bird/dev_20240627/dev_tables.json") as f:
            dev_tables = json.load(f)
        return dev_tables
    


    def __load_questions_dict(self) -> dict:
        parent_dir = dirname(dirname(dirname(abspath(__file__))))
        with open(f"{parent_dir}/benchmarks/bird/dev_20240627/dev.json") as f:
            dev_questions = json.load(f)
        return dev_questions
    


    def __load_active_database_questions(self) -> list:
        questions = []
        for q in self.questions_dict:
            if q["db_id"] == self.databases[self.active_database]:
                questions.append(q["question"])
        return questions