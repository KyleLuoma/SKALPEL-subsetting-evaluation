print(__name__)
from NlSqlBenchmark.snails.SnailsNlSqlBenchmark import SnailsNlSqlBenchmark
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark
from NlSqlBenchmark.spider.SpiderNlSqlBenchmark import SpiderNlSqlBenchmark
from NlSqlBenchmark.spider2.Spider2NlSqlBenchmark import Spider2NlSqlBenchmark
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark

class NlSqlBenchmarkFactory:
    
    benchmark_register = [
        "snails",
        "spider",
        "spider2",
        "bird",
        "abstract"
    ]

    benchmark_build_dict = {
        "snails": (SnailsNlSqlBenchmark, {"kill_container_on_exit": False, "db_host_profile": "sqlite", "sql_dialect": "sqlite"}),
        # "spider": (SpiderNlSqlBenchmark, {}),
        "spider2": (Spider2NlSqlBenchmark, {}),
        "bird": (BirdNlSqlBenchmark, {}),
        "abstract": (NlSqlBenchmark, {})
    }

    def __init__(self):
        self.benchmark_lookup = self._init_benchmark_lookup()

    # def build_benchmark(self, benchmark_name: str):
    #     assert benchmark_name in NlSqlBenchmarkFactory.benchmark_register
    #     if benchmark_name == "snails":
    #         return SnailsNlSqlBenchmark(kill_container_on_exit=False)
    #     elif benchmark_name == "spider":
    #         return SpiderNlSqlBenchmark()
    #     elif benchmark_name == "spider2":
    #         return Spider2NlSqlBenchmark()
    #     elif benchmark_name == "bird":
    #         return BirdNlSqlBenchmark()
        
    def build_benchmark(self, benchmark_name: str, benchmark_init_args: dict = None) -> NlSqlBenchmark:
        assert benchmark_name in NlSqlBenchmarkFactory.benchmark_register
        benchmark_class, init_args = NlSqlBenchmarkFactory.benchmark_build_dict[benchmark_name]
        if benchmark_init_args != None:
            init_args = benchmark_init_args
        return benchmark_class(**init_args)
        
    def lookup_benchmark_by_db_name(self, db_name: str) -> str:
        return self.benchmark_lookup[db_name]
    
    def _init_benchmark_lookup(self) -> dict:
        benchmark_lookup = {}
        for bm_name in NlSqlBenchmarkFactory.benchmark_build_dict.keys():
            bm: NlSqlBenchmark = NlSqlBenchmarkFactory.benchmark_build_dict[bm_name][0]
            databases = bm.get_database_names()
            for db in databases:
                assert db not in benchmark_lookup.items()
                benchmark_lookup[db] = bm_name
        return benchmark_lookup