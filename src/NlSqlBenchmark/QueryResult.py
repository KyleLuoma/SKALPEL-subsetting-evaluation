

class QueryResult:

    def __init__(
        self,
        result_set: dict,
        database: str,
        question: int,
        error_message: str = None
    ):

        self.result_set = result_set
        self.database = database
        self.question = question
        self.error_message = error_message

    def __eq__(self, other):
        return (
            self.result_set == other.result_set
            and self.database == other.database
            and self.question == other.question
            and self.error_message == other.error_message
        )

    def __getitem__(self, item_key):
        if item_key == "result_set":
            return self.result_set
        if item_key == "database":
            return self.database
        if item_key == "question":
            return self.question
        if item_key == "error_message":
            return self.error_message