"""
Super class for schema subsetting methods we evaluate in the SKALPEL project
"""

from NlSqlBenchmark import NlSqlBenchmark

class SchemaSubsetter:

    name = "abstract"
    
    def __init__(
            self,
            benchmark: NlSqlBenchmark.NlSqlBenchmark
            ):
        self.benchmark = benchmark

    def get_schema_subset(
            self, 
            question: str, 
            full_schema: dict
            ) -> dict:
        """
        'Abstract' method for subset generation

        Parameters
        ----------
        question: str
            The NL question to use for determining the required identifiers
        full_schema: dict
            Full schema representation comforing to the format:
                {
                "database": "",
                "tables[{
                    "name": "", 
                    "columns": [{"name": "", "type": ""}, ...]
                    }], ...}
        """
        return {
            "database": "database1",
            "tables": [
                {
                    "name": "table1",
                    "columns": [
                        {
                            "name": "column1",
                            "type": "int"
                        }
                    ]
                }
            ]
        }