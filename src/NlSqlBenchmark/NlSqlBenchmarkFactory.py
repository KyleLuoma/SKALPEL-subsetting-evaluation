print(__name__)
from NlSqlBenchmark.snails.SnailsNlSqlBenchmark import SnailsNlSqlBenchmark
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark

class NlSqlBenchmarkFactory:
    
    benchmark_register = [
        "snails",
        "spider",
        "bird"
    ]

    def __init__(self):
        pass

    def build_benchmark(self, benchmark_name):
        assert benchmark_name in NlSqlBenchmarkFactory.benchmark_register
        if benchmark_name == "snails":
            return SnailsNlSqlBenchmark(kill_container_on_exit=False)
        elif benchmark_name == "spider":
            return None
        elif benchmark_name == "bird":
            return BirdNlSqlBenchmark()
