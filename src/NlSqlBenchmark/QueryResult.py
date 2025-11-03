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
        if type(self) != type(other):
            return False
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
        raise KeyError(item_key)
    

    def __str__(self):
        s = f"##### QueryResult #####\nDatabase: {self.database}\n"
        s += f"Question: {self.question}\n"
        if self.result_set == None:
            s += f"Error: {self.error_message}"
            return s
        s += "Result Set Dict:\n"
        s += str(self.result_set)
        return s
    



