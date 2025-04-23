from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion

class RslSqlSubsetter(SchemaSubsetter):
    """
    Skalpel subsetter wrapper for RSL-SQL (https://github.com/Laqcce-cao/RSL-SQL)
    """
    name = "rslsql"

    def __init__(self, benchmark: NlSqlBenchmark):
        self.name = RslSqlSubsetter.name
        self.benchmark = benchmark


    def preprocess_databases(
            self, 
            exist_ok: bool = True, 
            filename_comments: str = "", 
            skip_already_processed: bool = False, 
            **args
            ):
        raise NotImplementedError


    def get_schema_subset(self, benchmark_question: BenchmarkQuestion):
        raise NotImplementedError
    
