"""
Wrapper class for the CHESS subsetter
https://github.com/ShayanTalaei/CHESS
"""

from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark

class ChessSubsetter(SchemaSubsetter):

    def __init__(
            self,
            benchmark: NlSqlBenchmark
    ):
        pass

    def get_schema_subset(self, question: str, full_schema: dict) -> dict:
        return super().get_schema_subset(question, full_schema)
    

    