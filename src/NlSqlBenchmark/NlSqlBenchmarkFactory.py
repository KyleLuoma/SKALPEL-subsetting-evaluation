print(__name__)
from NlSqlBenchmark.SnailsNlSqlBenchmark import SnailsNlSqlBenchmark

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
            return SnailsNlSqlBenchmark()
        elif benchmark_name == "spider":
            return None
        elif benchmark_name == "bird":
            return None
