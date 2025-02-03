from os.path import dirname, abspath
import time

import docker
import docker.errors
import docker.models
import docker.models.containers

from NlSqlBenchmark.snails.util import db_util, load_nl_questions
from NlSqlBenchmark.QueryResult import QueryResult
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion




class SnailsNlSqlBenchmark(NlSqlBenchmark):

    name = "snails"
    
    def __init__(
            self, 
            db_host_profile: str = "docker",
            kill_container_on_exit: bool = True,
            verbose: bool = False
            ):
        super().__init__()
        self.verbose = verbose
        self.benchmark_folder = dirname(dirname(dirname(dirname(abspath(__file__))))) + "/benchmarks/snails"
        self.kill_container_on_exit = kill_container_on_exit
        self.container = None
        if db_host_profile == "docker":
            self.db_info_file = self.benchmark_folder + "/ms_sql/dbinfo.json"
            self.container = self._init_docker()
        self.name = "snails"
        self.naturalness = "Native"
        self.syntax = "tsql"
        self.databases = [
            "ASIS_20161108_HerpInv_Database",
            "ATBI",
            "CratersWildlifeObservations",
            "KlamathInvasiveSpecies",
            "NorthernPlainsFireManagement",
            "NTSB",
            "NYSED_SRC2022",
            "PacificIslandLandbirds",
            "SBODemoUS"
        ]
        self.sql_dialect = "mssql"
        self.schema_cache = {}
        self.active_database_questions = self.__load_active_database_questions()
        self.active_database_queries = self.__load_active_database_queries()
        

        


    def __del__(self):
        if self.kill_container_on_exit and self.container != None:
            print("SnailsNlSqlBenchmark is stopping Docker container 'skalpel-running-snails'")
            try:
                self.container.kill()
            except docker.errors.APIError as e:
                pass


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
        question = self.get_active_question()
        self.active_question_no += 1
        return question
    


    def __len__(self):
        return 503

    

    def _init_docker(self, retry_attempts: int = 5) -> docker.models.containers.Container:

        database_running = True
        client = docker.from_env()
        try:
            self.execute_query("select 1", database="ATBI")
            container = client.containers.get("skalpel-running-snails")
        except db_util.pyodbc.OperationalError as e:
            if self.verbose:
                time.sleep(1)
                print(e)
            database_running = False

        retry_counts = 0

        while not database_running and retry_counts < retry_attempts:    
            retry_counts += 1
            try:
                client.images.get('snails-db')
            except docker.errors.ImageNotFound:
                if self.verbose:
                    print("The 'snails-db' Docker image is not available locally. Run the install script /benchmarks/snails/ms_sql/install_snails_db.sh before running the SNAILS benchmark.")
                raise
            try:
                if self.verbose:
                    print("Loading Docker container 'skaplel-running-snails'")
                container = client.containers.get("skalpel-running-snails")
                container.start()
            except docker.errors.NotFound:
                if self.verbose:
                    print("Container not found, running container for the first time on port 1433.")
                container = client.containers.run("snails-db", ports={1433:1433}, name="skalpel-running-snails", detach=True)
                

            database_running = True
            try:
                self.execute_query("select 1", database="ATBI")
            except db_util.pyodbc.OperationalError as e:
                if self.verbose:
                    print(e)
                time.sleep(1)
                database_running = False
            
        return container
    


    def _load_schema(
            self, 
            database_name: str
            ) -> Schema:
        schema_name = "dbo"
        if self.naturalness == "Regular":
            schema_name = "Regular_Naturalness"
        elif self.naturalness == "Low":
            schema_name = "Low_Naturalness"
        elif self.naturalness == "Least":
            schema_name = "Least_Naturalness"

        schema = Schema(database=database_name, tables=[])
            
        tables_and_columns = db_util.get_tables_and_columns_from_sql_server_db(
            db_name=database_name,
            schema=schema_name,
            db_list_file=self.db_info_file
            )

        pk_lookup, fk_lookup = self._make_pk_fk_lookups(database_name=database_name)

        for table in tables_and_columns:
            if table in pk_lookup:
                pk_list = pk_lookup[table]
            else:
                pk_list = []
            if table in fk_lookup:
                fk_list = fk_lookup[table]
            else:
                fk_list = []
            schema_table = SchemaTable(
                name=table,
                columns=[],
                primary_keys=pk_list,
                foreign_keys=fk_list
            )
            for column in tables_and_columns[table]:
                schema_table.columns.append(
                    TableColumn(name=column.split(" ")[0], data_type=column.split(" ")[1])
                )
            schema.tables.append(schema_table)
        return schema



    def _make_pk_fk_lookups(self, database_name: str) -> (dict, dict):
        with open(f"./src/NlSqlBenchmark/snails/util/get_fk_pk_from_mssql.sql", "r") as f:
            pk_fk_query = f.read()

        pk_fk = db_util.do_query(
            query=pk_fk_query,
            database_name=database_name,
            db_list_file=self.db_info_file
        )

        pk_lookup = {}
        fk_lookup = {}

        for i in range(0, len(pk_fk["pk_table"])):
            pk_table = pk_fk["pk_table"][i]
            if pk_table not in pk_lookup:
                pk_lookup[pk_table] = [pk_fk["pk_column"][i]]
            elif pk_fk["pk_column"][i] not in pk_lookup[pk_table]:
                pk_lookup[pk_table].append(pk_fk["pk_column"][i])
            fk_table = pk_fk["fk_table"][i]
            if fk_table not in fk_lookup:
                fk_lookup[fk_table] = [ForeignKey(
                    columns=[pk_fk["fk_column"][i]], references=(pk_fk["pk_table"][i], pk_fk["pk_column"][i])
                    )]
            else:
                fk_lookup[fk_table].append(ForeignKey(
                    columns=[pk_fk["fk_column"][i]], references=(pk_fk["pk_table"][i], pk_fk["pk_column"][i])
                    ))

        return pk_lookup, fk_lookup


    def __load_active_database_questions(self) -> list:
        db_name = self.databases[self.active_database]
        if "SBODemoUS" not in db_name:
            qnl_dict = load_nl_questions.load_nlq_into_df(
                filename=f"{self.databases[self.active_database]}_{self.naturalness}.sql",
                filepath=self.benchmark_folder + "/nlq_sql/tsql/"
                )
            return qnl_dict["question"]
        else:
            questions, queries = self._load_and_consolidate_sbodemo()
            return questions
    

    def __load_active_database_queries(self) -> list:
        db_name = self.databases[self.active_database]
        if "SBODemoUS" not in db_name:
            qnl_dict = load_nl_questions.load_nlq_into_df(
                filename=f"{self.databases[self.active_database]}_{self.naturalness}.sql",
                filepath=self.benchmark_folder + "/nlq_sql/tsql/"
                )
            return qnl_dict["query_gold"]
        else:
            questions, queries = self._load_and_consolidate_sbodemo()
            return queries


    def _load_and_consolidate_sbodemo(self) -> (list, list):
        modules = [
            "SBODemoUS-Banking",
            "SBODemoUS-Business Partners",
            "SBODemoUS-Finance",
            "SBODemoUS-General",
            "SBODemoUS-Human Resources",
            "SBODemoUS-Inventory and Production",
            "SBODemoUS-Reports",
            "SBODemoUS-Sales Opportunities",
            "SBODemoUS-Service"
        ]
        questions = []
        queries = []
        for module in modules:
            qnl_dict = load_nl_questions.load_nlq_into_df(
                filename=f"{module}_{self.naturalness}.sql",
                filepath=self.benchmark_folder + "/nlq_sql/tsql/"
            )
            questions += qnl_dict["question"]
            queries += qnl_dict["query_gold"]
        return questions, queries


    def set_naturalness(self, naturalness: str) -> None:
        """
        Sets the naturalness level for the benchmark.
        Parameters:
        naturalness (str): The naturalness level to set. Must be one of ["Regular", "Low", "Least", "Native"].
        Raises:
        ValueError: If the provided naturalness level is not one of the allowed values.
        """
        if naturalness not in ["Regular", "Low", "Least", "Native"]:
            raise ValueError
        self.naturalness = naturalness



    def set_syntax(self, syntax: str) -> None:
        """
        Set the syntax type for the benchmark.
        Parameters:
        syntax (str): The syntax type to set. Must be either "tsql" or "sqlite".
        Raises:
        ValueError: If the provided syntax is not "tsql" or "sqlite".
        """
        if syntax not in ["tsql", "sqlite"]:
            raise ValueError
        self.syntax = syntax



    def set_active_schema(self, database_name: str) -> None:
        schema_lookup = {k: v for v, k in enumerate(self.databases)}
        self.active_database = schema_lookup[database_name]
        self.active_database_questions = self.__load_active_database_questions()
        self.active_database_queries = self.__load_active_database_queries()
    


    def get_active_question(self) -> BenchmarkQuestion:
        return super().get_active_question()
    


    def get_active_schema(self, database: str = None) -> Schema:
        if not database:
            database = self.databases[self.active_database]
        if database in self.schema_cache.keys():
            return self.schema_cache[database]
        else: 
            schema = self._load_schema(database_name=database)
            self.schema_cache[database] = schema
            return schema 


    def execute_query(self, query: str, database: str = None, question: int = None) -> QueryResult:
        if database == None:
            database = self.databases[self.active_database]
        try:
            result_set_dict = db_util.do_query(
                query=query,
                database_name=database,
                db_list_file=self.db_info_file
            )
        except db_util.pyodbc.ProgrammingError as e:
            return QueryResult(
                result_set=None,
                database=None,
                question=None,
                error_message=str(e)
            )
        return QueryResult(
            result_set = result_set_dict,
            database = database,
            question = question
        )
    

    def get_sample_values(self, table_name: str, column_name: str, num_values: int = 2, database: str = None):
        query = f"select top {num_values} [{column_name}] from [{table_name}]"
        result = self.execute_query(query=query, database=database)
        return result.result_set[column_name]
