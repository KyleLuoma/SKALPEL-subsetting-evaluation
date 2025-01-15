from NlSqlBenchmark.snails.SnailsNlSqlBenchmark import SnailsNlSqlBenchmark
import docker


def docker_init_test():
    snails = SnailsNlSqlBenchmark(db_host_profile="docker")
    return snails.container != None and type(snails.container) == docker.models.containers.Container


def docker_db_query_test():
    snails = SnailsNlSqlBenchmark(db_host_profile="docker")
    result = snails.execute_query(
        query="select * from tlu_DecayStage;",
        database="ATBI"
    )
    return "DecayStage_ID" in result["result_set"]