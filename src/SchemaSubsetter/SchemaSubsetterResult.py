from NlSqlBenchmark.SchemaObjects import Schema

class SchemaSubsetterResult:

    def __init__(self, schema_subset: Schema, prompt_tokens: int = 0, error_message: str = None, raw_llm_response: str = None):
        self.schema_subset = schema_subset
        self.prompt_tokens = prompt_tokens
        self.error_message = error_message
        self.raw_llm_response = raw_llm_response

    def __getitem__(self, item_key):
        return getattr(self, item_key, None)
    
