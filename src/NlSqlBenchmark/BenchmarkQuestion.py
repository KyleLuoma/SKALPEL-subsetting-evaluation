from NlSqlBenchmark.SchemaObjects import Schema

class BenchmarkQuestion:

    def __init__(
            self,
            question: str,
            query: str,
            question_number: int,
            schema: Schema
            ):
        self.question = question
        self.query = query
        self.question_number = question_number
        self.schema = schema


    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return (
            self.question == other.question
            and self.query == other.query
            and self.question_number == other.question_number
            and self.schema == other.schema
        )


    def __getitem__(self, item_key):
        if item_key == "question":
            return self.question
        if item_key == "query":
            return self.query
        if item_key == "question_number":
            return self.question_number
        if item_key == "schema":
            return self.schema
        raise KeyError(item_key)
    