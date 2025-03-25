from NlSqlBenchmark.snails.SnailsNlSqlBenchmark import SnailsNlSqlBenchmark
from NlSqlBenchmark.QueryResult import QueryResult

import docker


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
    snails = SnailsNlSqlBenchmark(db_host_profile="docker", kill_container_on_exit=False)
    return snails.container != None and type(snails.container) == docker.models.containers.Container


def docker_db_query_test():
    snails = SnailsNlSqlBenchmark(db_host_profile="docker", kill_container_on_exit=False)
    result = snails.execute_query(
        query="select * from tlu_DecayStage;",
        database="ATBI"
    )
    return "DecayStage_ID" in result["result_set"]


def execute_query_syntax_error_test():
    snails = SnailsNlSqlBenchmark(db_host_profile="docker", kill_container_on_exit=False)
    query = "SELECT * from tlu_DecayStage WHRER TRUE"
    correct_result = QueryResult(
        result_set=None, 
        database=None, 
        question=None, 
        error_message='(\'42000\', "[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Incorrect syntax near \'TRUE\'. (102) (SQLExecDirectW)")'
    )
    result = snails.execute_query(
        query=query,
        database="ATBI"
    )
    return result == correct_result



def make_pk_fk_lookups_test():
    snails = SnailsNlSqlBenchmark(db_host_profile="docker", kill_container_on_exit=False)
    pk, fk = snails._make_pk_fk_lookups("ATBI")
    return (
        len(pk) == 15
        and len(fk) == 10
    )

def load_schema_test():
    snails = SnailsNlSqlBenchmark(db_host_profile="docker", kill_container_on_exit=False)
    schema = snails._load_schema("ATBI")
    return (
        len(schema.tables) == 28
        and len(schema.tables[0].columns) == 10
    )


def set_and_get_active_schema_test():
    snails = SnailsNlSqlBenchmark(db_host_profile="docker", kill_container_on_exit=False)
    snails.set_active_schema("NTSB")
    schema = snails.get_active_schema()
    return len(schema.tables) == 40


def iter_test():
    snails = SnailsNlSqlBenchmark(db_host_profile="docker", kill_container_on_exit=False)
    found_databases = set()
    questions = []
    for q in snails:
        found_databases.add(q.schema.database)
        questions.append(1)
    return found_databases == snails_databases and len(questions) == 503


def get_sample_values_test():
    correct_result = ['1', '2']
    snails = SnailsNlSqlBenchmark(db_host_profile="docker", kill_container_on_exit=False)
    result = snails.get_sample_values(
        table_name="tlu_DecayStage", 
        column_name="DecayStage_ID", 
        database="ATBI"
        )
    return len(result) == len(correct_result) and set(result) == set(correct_result)


def get_id_col_unique_values_test():
    snails = SnailsNlSqlBenchmark(db_host_profile="docker", kill_container_on_exit=False)
    unique_values = snails.get_unique_values(
        table_name="tlu_DecayStage", 
        column_name="DecayStage_ID", 
        database="ATBI"
    )
    return len(unique_values) == 0


def get_unique_values_test():
    snails = SnailsNlSqlBenchmark(db_host_profile="docker", kill_container_on_exit=False)
    unique_values = snails.get_unique_values(
        table_name="tlu_DecayStage", 
        column_name="DecayStage_Descr", 
        database="ATBI"
    )
    return len(unique_values) == 6