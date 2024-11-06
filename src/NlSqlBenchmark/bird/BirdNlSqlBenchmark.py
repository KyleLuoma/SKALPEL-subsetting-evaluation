from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
import json
import sqlite3
from os.path import dirname, abspath

class BirdNlSqlBenchmark(NlSqlBenchmark):

    name = "BIRD"
    
    def __init__(self):
        super().__init__()
        self.benchmark_folder = dirname(dirname(dirname(dirname(abspath(__file__))))) + "/benchmarks/bird/dev_20240627"
        self.tables_dict = self.__load_tables_dict()
        self.questions_list = self.__load_questions_list()
        self.databases = [t["db_id"] for t in self.tables_dict]
        self.active_database_questions = self.__load_active_database_questions()
        self.active_database_queries = self.__load_active_database_queries()
        self.name = "bird"

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



    def get_active_question(self) -> dict:
        return super().get_active_question()



    def get_active_schema(self, database: str = None) -> dict:
        if database == None:
            database = self.databases[self.active_database]
        for s in self.tables_dict:
            if s["db_id"] == database:
                schema_dict = s
                break
        active_schema = {
            "database": database,
            "tables": []
            }
        for i in range(0, len(schema_dict["table_names_original"])):
            # Get table names
            active_schema["tables"].append({"name": schema_dict["table_names_original"][i]})
            columns = []
            # get column names
            for (c, t) in zip(schema_dict["column_names_original"], schema_dict["column_types"]):
                if c[0] == i:
                    columns.append({"name": c[1], "type": t})
            active_schema["tables"][i]["columns"] = columns
            active_schema["tables"][i]["primary_keys"] = []
            active_schema["tables"][i]["foreign_keys"] = []
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
                    active_schema["tables"][i]["primary_keys"].append(composit_col_names)
            # Get foreign keys
            for fk_reference in schema_dict["foreign_keys"]:
                fk_dict = {"columns": [], "references": None}
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
                    fk_dict["columns"].append(
                        schema_dict["column_names_original"][key_column][1]
                    )
                referenced_table_name = schema_dict["table_names_original"][
                        schema_dict["column_names_original"][ref_pk[0]][0]
                    ]
                referenced_columns = []
                for key_column in ref_pk:
                        referenced_columns.append(schema_dict["column_names_original"][key_column][1])
                fk_dict["references"] = (referenced_table_name, referenced_columns)
                if len(fk_dict["columns"]) > 0:
                    active_schema["tables"][i]["foreign_keys"].append(fk_dict)

        return active_schema
    


    def set_active_schema(self, database_name: str) -> None:
        schema_lookup = {k: v for v, k in enumerate(self.databases)}
        self.active_database = schema_lookup[database_name]
        self.active_database_questions = self.__load_active_database_questions()
        self.active_database_queries = self.__load_active_database_queries()
    


    def execute_query(self, query: str, database: str = None, question: int = None) -> dict:
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
        except sqlite3.OperationalError as e:
            return {
                "result_set": None,
                "database": None,
                "question": None,
                "error_message": str(e)
            }
        result_list = res.fetchall()
        columns = [d[0] for d in res.description]
        result_set_dict = {}
        for i, c in enumerate(columns):
            values = [t[i] for t in result_list]
            result_set_dict[c] = values
        return {
            "result_set": result_set_dict,
            "database": database,
            "question": question,
            "error_message": ""
        }   
    

    def get_sample_values(self,  table_name: str, column_name: str, database: str = None, num_values: int = 2) -> list:
        if database == None:
            database = self.active_database
        con = sqlite3.connect(
            f"{self.benchmark_folder}/dev_databases/dev_databases/{database}/{database}.sqlite"
            )
        cur = con.cursor()
        query = f"select `{column_name}` from `{table_name}` limit {num_values}"
        res = cur.execute(query)
        sample_values = res.fetchall()
        return [s[0] for s in sample_values]


    def __load_tables_dict(self) -> dict:
        with open(f"{self.benchmark_folder}/dev_tables.json") as f:
            dev_tables = json.load(f)
        return dev_tables
    


    def __load_questions_list(self) -> list:
        with open(f"{self.benchmark_folder}/dev.json") as f:
            dev_questions = json.load(f)
        return dev_questions
    


    def __load_active_database_questions(self) -> list:
        questions = []
        for q in self.questions_list:
            if q["db_id"] == self.databases[self.active_database]:
                questions.append(q["question"])
        return questions
    


    def __load_active_database_queries(self) -> list:
        queries = []
        for q in self.questions_list:
            if q["db_id"] == self.databases[self.active_database]:
                queries.append(q["SQL"])
        return queries
    

