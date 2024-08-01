"""
Super class for schema subsetting methods we evaluate in the SKALPEL project
"""

class SchemaSubsetter:
    
    def __init__(self):
        pass

    def get_schema_subset(self, question: str, full_schema: dict) -> dict:
        """
        'Abstract' method for subset generation

        Parameters
        ----------
        question: str
            The NL question to use for determining the required identifiers
        full_schema: dict
            Full schema representation comforing to the format:
                {"tables[{
                    "name": "", 
                    "columns": [{"name": "", "type": ""}, ...]
                    }], ...}
        """
        return {
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