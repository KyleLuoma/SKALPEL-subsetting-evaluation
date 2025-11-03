"""
Copyright 2025 Kyle Luoma

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from NlSqlBenchmark.SchemaObjects import Schema

class SchemaSubsetterResult:

    def __init__(self, schema_subset: Schema, prompt_tokens: int = 0, error_message: str = None, raw_llm_response: str = None):
        self.schema_subset = schema_subset
        self.prompt_tokens = prompt_tokens
        self.error_message = error_message
        self.raw_llm_response = raw_llm_response

    def __getitem__(self, item_key):
        return getattr(self, item_key, None)
    
