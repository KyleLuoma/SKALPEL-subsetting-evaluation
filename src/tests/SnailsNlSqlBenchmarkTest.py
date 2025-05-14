from NlSqlBenchmark.snails.SnailsNlSqlBenchmark import SnailsNlSqlBenchmark
from NlSqlBenchmark.QueryResult import QueryResult

import docker

DB_HOST_PROFILE = "sqlite"
SQL_DIALECT = "sqlite"

snails_databases = {
    "ASIS_20161108_HerpInv_Database",
    "ATBI",
    "CratersWildlifeObservations",
    "KlamathInvasiveSpecies",
    "NorthernPlainsFireManagement",
    "NTSB",
    "NYSED_SRC2022",
    "PacificIslandLandbirds",
    "SBODemoUS"
}

def docker_init_test():
    if DB_HOST_PROFILE != "docker":
        return True
    snails = SnailsNlSqlBenchmark(db_host_profile=DB_HOST_PROFILE, kill_container_on_exit=False, sql_dialect=SQL_DIALECT)
    return snails.container != None and type(snails.container) == docker.models.containers.Container


def docker_db_query_test():
    if DB_HOST_PROFILE != "docker":
        return True
    snails = SnailsNlSqlBenchmark(db_host_profile=DB_HOST_PROFILE, kill_container_on_exit=False, sql_dialect=SQL_DIALECT)
    result = snails.execute_query(
        query="select * from tlu_DecayStage;",
        database="ATBI"
    )
    return "DecayStage_ID" in result["result_set"]


def execute_query_syntax_error_test():
    snails = SnailsNlSqlBenchmark(db_host_profile=DB_HOST_PROFILE, kill_container_on_exit=False, sql_dialect=SQL_DIALECT)
    query = "SELECT * from tlu_DecayStage WHRER TRUE"
    if SQL_DIALECT == "sqlite":
        error_message = 'near "TRUE": syntax error'
    elif SQL_DIALECT == "mssql":
        error_message='(\'42000\', "[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Incorrect syntax near \'TRUE\'. (102) (SQLExecDirectW)")'
    correct_result = QueryResult(
        result_set=None, 
        database=None, 
        question=None, 
        error_message=error_message
    )
    result = snails.execute_query(
        query=query,
        database="ATBI"
    )
    return result == correct_result



def make_pk_fk_lookups_test():
    snails = SnailsNlSqlBenchmark(db_host_profile=DB_HOST_PROFILE, kill_container_on_exit=False, sql_dialect=SQL_DIALECT)
    pk, fk = snails._make_pk_fk_lookups("ATBI")
    if SQL_DIALECT=="mssql":
        return (
            len(pk) == 15
            and len(fk) == 10
        )
    else:
        return (
            len(pk) == 14
            and len(fk) == 9
        )

def load_schema_test():
    snails = SnailsNlSqlBenchmark(db_host_profile=DB_HOST_PROFILE, kill_container_on_exit=False, sql_dialect=SQL_DIALECT)
    schema = snails._load_schema("ATBI")
    return (
        len(schema.tables) == 28
    )


def set_and_get_active_schema_test():
    snails = SnailsNlSqlBenchmark(db_host_profile=DB_HOST_PROFILE, kill_container_on_exit=False, sql_dialect=SQL_DIALECT)
    snails.set_active_schema("NTSB")
    schema = snails.get_active_schema()
    return len(schema.tables) == 40


def iter_test():
    snails = SnailsNlSqlBenchmark(db_host_profile=DB_HOST_PROFILE, kill_container_on_exit=False, sql_dialect=SQL_DIALECT)
    found_databases = set()
    questions = []
    for q in snails:
        found_databases.add(q.schema.database)
        questions.append(1)
    return found_databases == snails_databases and len(questions) == 503


def iter_reset_test():
    snails = SnailsNlSqlBenchmark(db_host_profile=DB_HOST_PROFILE, kill_container_on_exit=False, sql_dialect=SQL_DIALECT)
    for q in snails:
        pass
    question = snails.get_active_question()
    return question.question_number == 0 and question.schema.database == snails.databases[snails.active_database]

def get_sample_values_test():
    correct_result = ['1', '2']
    snails = SnailsNlSqlBenchmark(db_host_profile=DB_HOST_PROFILE, kill_container_on_exit=False, sql_dialect=SQL_DIALECT)
    result = snails.get_sample_values(
        table_name="tlu_DecayStage", 
        column_name="DecayStage_ID", 
        database="ATBI"
        )
    return len(result) == len(correct_result) and set(result) == set(correct_result)


def get_id_col_unique_values_test():
    snails = SnailsNlSqlBenchmark(db_host_profile=DB_HOST_PROFILE, kill_container_on_exit=False, sql_dialect=SQL_DIALECT)
    unique_values = snails.get_unique_values(
        table_name="tlu_DecayStage", 
        column_name="DecayStage_ID", 
        database="ATBI"
    )
    return len(unique_values) == 0


def get_unique_values_test():
    snails = SnailsNlSqlBenchmark(db_host_profile=DB_HOST_PROFILE, kill_container_on_exit=False, sql_dialect=SQL_DIALECT)
    unique_values = snails.get_unique_values(
        table_name="tlu_DecayStage", 
        column_name="DecayStage_Descr", 
        database="ATBI"
    )
    return len(unique_values) == 6