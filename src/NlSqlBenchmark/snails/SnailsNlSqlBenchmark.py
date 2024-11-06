from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark

class SnailsNlSqlBenchmark(NlSqlBenchmark):

    name = "SNAILS"
    
    def __init__(self):
        super().__init__()
        self.name = "SNAILS"
        self.naturalness = "Native"
        self.syntax = "tsql"
        pass



    def __iter__(self):
        return self
    


    def __next__(self):
        question = self.get_active_question()
        self.active_question_no += 1
        return question
    


    def __len__(self):
        return 0
    


    def set_naturalness(self, naturalness: str) -> None:
        """
        Sets the naturalness level for the benchmark.
        Parameters:
        naturalness (str): The naturalness level to set. Must be one of ["Regular", "Low", "Least", "Native"].
        Raises:
        ValueError: If the provided naturalness level is not one of the allowed values.
        """
        if naturalness not in ["Regular", "Low", "Least", "Native"]:
            raise ValueError
        self.naturalness = naturalness



    def set_syntax(self, syntax: str) -> None:
        """
        Set the syntax type for the benchmark.
        Parameters:
        syntax (str): The syntax type to set. Must be either "tsql" or "sqlite".
        Raises:
        ValueError: If the provided syntax is not "tsql" or "sqlite".
        """
        if syntax not in ["tsql", "sqlite"]:
            raise ValueError
        self.syntax = syntax
    


    def get_active_question(self) -> dict:
        return super().get_active_question()
    


    def get_active_schema(self, database: str = None) -> dict:
        return super().get_active_schema(database)