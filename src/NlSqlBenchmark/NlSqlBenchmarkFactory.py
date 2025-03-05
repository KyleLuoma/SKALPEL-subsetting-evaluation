print(__name__)
from NlSqlBenchmark.snails.SnailsNlSqlBenchmark import SnailsNlSqlBenchmark
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark
from NlSqlBenchmark.spider.SpiderNlSqlBenchmark import SpiderNlSqlBenchmark
from NlSqlBenchmark.spider2.Spider2NlSqlBenchmark import Spider2NlSqlBenchmark

class NlSqlBenchmarkFactory:
    
    benchmark_register = [
        "snails",
        "spider",
        "spider2",
        "bird"
    ]

    def __init__(self):
        pass

    def build_benchmark(self, benchmark_name):
        assert benchmark_name in NlSqlBenchmarkFactory.benchmark_register
        if benchmark_name == "snails":
            return SnailsNlSqlBenchmark(kill_container_on_exit=False)
        elif benchmark_name == "spider":
            return SpiderNlSqlBenchmark()
        elif benchmark_name == "spider2":
            return Spider2NlSqlBenchmark()
        elif benchmark_name == "bird":
            return BirdNlSqlBenchmark()
