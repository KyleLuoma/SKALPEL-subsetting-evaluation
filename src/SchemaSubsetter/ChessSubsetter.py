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

class ChessSubsetter(SchemaSubsetter):

    def __init__(
            self,
            benchmark: NlSqlBenchmark,
            do_preprocessing: bool = False
    ):
        pass

    def get_schema_subset(self, benchmark_question: BenchmarkQuestion) -> Schema:
        """
        Replicates information retriever and schema selector agents in isolation from the full CHESS team context
        """
        tentative_schema: Dict[str, List[str]]
        keywords: List[str] = []
        # Keyword generation agent behavior goes here

        similar_columns: Dict[str, List[str]] = {}
        # Context and entity retrieval behavior goes here


        
    