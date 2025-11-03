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

class ValueReferenceProblemItem:

    def __init__(
            self,
            table_name: str,
            column_name: str,
            db_text_value: str,
            nlq_ngram: str,
            ):
        self.table_name = table_name
        self.column_name = column_name
        self.db_text_value = db_text_value
        self.nlq_ngram = nlq_ngram


    
    def __str__(self):
        return str({
            "table_name": self.table_name,
            "column_name": self.column_name,
            "db_text_value": self.db_text_value,
            "nlq_ngram": self.nlq_ngram
        })
    

    def __getitem__(self, item_key):
        if item_key == "table_name":
            return self.table_name
        if item_key == "column_name":
            return self.column_name
        if item_key == "db_text_value":
            return self.db_text_value
        if item_key == "nlq_ngram":
            return self.nlq_ngram
        raise KeyError(f"Key {item_key} not found in ValueReferenceProblemTable")
    

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return (
            self.table_name == other.table_name
            and self.column_name == other.column_name
            and self.db_text_value == other.db_text_value
            and self.nlq_ngram == other.nlq_ngram
        )
    


class ValueReferenceProblemResults:

    def __init__(
            self, 
            problem_columns: list[ValueReferenceProblemItem]
            ):
        self.problem_columns = problem_columns
   

    def get_unmatched_column_names_as_set(self) -> set[str]:
        column_names = set()
        for column in self.problem_columns:
            column_names.add(f"{column.table_name}.{column.column_name}")
        return column_names


    def to_dict(self):
        return_dict = {
            "table_name": [],
            "column_name": [],
            "text_value": [],
            "nlq_ngram": []
        }
        for column in self.problem_columns:
            return_dict["table_name"].append(column.table_name)
            return_dict["column_name"].append(column.column_name)
            return_dict["text_value"].append(column.db_text_value)
            return_dict["nlq_ngram"].append(column.nlq_ngram)
        return return_dict

