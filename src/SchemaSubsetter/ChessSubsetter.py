"""
Wrapper class for the CHESS subsetter
https://github.com/ShayanTalaei/CHESS
"""

from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from typing import List, Dict, Any

from SchemaSubsetter.CHESS.src import preprocess
import time

class ChessSubsetter(SchemaSubsetter):

    def __init__(
            self,
            benchmark: NlSqlBenchmark,
            do_preprocessing: bool = False
    ):
        self.benchmark = benchmark
        self.vector_db_root = "./src/SchemaSubsetter/CHESS/processed_db"
        if do_preprocessing:
            s_time = time.perf_counter()
            self.preprocess_databases()
            e_time = time.perf_counter()
            print("Time to process", self.benchmark.name, "schemas was", str(e_time - s_time))


    def preprocess_databases(self):
        for database in self.benchmark.databases:
            preprocess.make_db_context_vec_db(
                db_id=database,
                db_directory_path=self.vector_db_root,
                benchmark=self.benchmark
            )


    def get_schema_subset(self, benchmark_question: BenchmarkQuestion) -> Schema:
        """
        Replicates information retriever and schema selector agents in isolation from the full CHESS team context
        """
        tentative_schema: Dict[str, List[str]]
        keywords: List[str] = []
        # Keyword generation agent behavior goes here

        similar_columns: Dict[str, List[str]] = {}
        # Context and entity retrieval behavior goes here


        
    