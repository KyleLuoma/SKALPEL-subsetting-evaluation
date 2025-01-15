from NlSqlBenchmark.snails.SnailsNlSqlBenchmark import SnailsNlSqlBenchmark


def docker_init_test():
    snails = SnailsNlSqlBenchmark(db_host_profile="docker")