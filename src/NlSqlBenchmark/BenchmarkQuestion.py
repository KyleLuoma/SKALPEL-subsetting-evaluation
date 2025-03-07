from NlSqlBenchmark.SchemaObjects import Schema

class BenchmarkQuestion:

    def __init__(
            self,
            question: str,
            query: str,
            query_dialect: str,
            question_number: int,
            schema: Schema,
            schema_naturalness: str = "Native"
            ):
        self.question = question
        self.query = query
        self.query_dialect = query_dialect
        self.question_number = question_number
        self.schema = schema
        self.schema_naturalness = schema_naturalness
        self.query_filename = None


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
            and self.query_filename == other.query_filname
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
        raise KeyError(item_key)
    