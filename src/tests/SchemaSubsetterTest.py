from SchemaSubsetter import SchemaSubsetter

def get_schema_subset_test():
    ss = SchemaSubsetter.SchemaSubsetter(benchmark=None)
    result = ss.get_schema_subset(
        question="why foo bar?",
        full_schema={}
    )
    return result == {
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