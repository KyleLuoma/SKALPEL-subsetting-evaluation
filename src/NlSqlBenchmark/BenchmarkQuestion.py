from NlSqlBenchmark.SchemaObjects import Schema
from copy import deepcopy

class BenchmarkQuestion:

    def __init__(
            self,
            question: str,
            query: str,
            query_dialect: str,
            question_number: int,
            schema: Schema,
            schema_naturalness: str = "Native",
            evidence: str = None,
            ):
        self.question = question
        self.query = query
        self.query_dialect = query_dialect
        self.question_number = question_number
        self.schema = schema
        self.schema_naturalness = schema_naturalness
        self.query_filename = None
        self.evidence = evidence


    def __eq__(self, other) -> bool:
        if type(self) != type(other):
            return False
        return (
            self.question == other.question
            and self.query == other.query
            and self.query_dialect == other.query_dialect
            and self.question_number == other.question_number
            and self.schema == other.schema
            and self.schema_naturalness == other.schema_naturalness
            and self.query_filename == other.query_filename
            and self.evidence == other.evidence
        )


    def copy(self):
        return BenchmarkQuestion(
            question=deepcopy(self.question),
            query=deepcopy(self.query),
            query_dialect=deepcopy(self.query_dialect),
            question_number=deepcopy(self.question_number),
            schema=deepcopy(self.schema),
            schema_naturalness=deepcopy(self.schema_naturalness),
            evidence=deepcopy(self.evidence)
        )


    def __getitem__(self, item_key):
        if item_key == "question":
            return self.question
        if item_key == "query":
            return self.query
        if item_key == "query_dialect":
            return self.query_dialect
        if item_key == "question_number":
            return self.question_number
        if item_key == "schema":
            return self.schema
        if item_key == "schema_naturalness":
            return self.schema_naturalness
        if item_key == "query_filename":
            return self.query_filename
        if item_key == "evidence":
            return self.evidence
        raise KeyError(item_key)
    