from NlSqlBenchmark.SchemaObjects import Schema

class SchemaSubsetterResult:

    def __init__(self, schema_subset: Schema, prompt_tokens: int = 0):
        self.schema_subset = schema_subset
        self.prompt_tokens = prompt_tokens

    def __getitem__(self, item_key):
        return getattr(self, item_key, None)
    
