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

class WordIdentifierDistance:

    def __init__(
            self,
            search_word: str,
            database_identifier: str,
            distance: float
            ):
        self.search_word = search_word
        self.database_identifier = database_identifier
        self.distance = distance


    def __str__(self):
        return str({
            "search_word": self.search_word,
            "database_identifier": self.database_identifier,
            "distance": self.distance
            })
    

    def __getitem__(self, item_key):
        if item_key == "search_word":
            return self.search_word
        if item_key == "database_identifier":
            return self.database_identifier
        if item_key == "distance":
            return self.distance
        

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return (
            self.search_word == other.search_word
            and self.database_identifier == other.database_identifier
            and self.distance == other.distance
        )


class VectorSearchResults:

    def __init__(
            self,
            search_word: str,
            database_name: str,
            distance_threshold: float
            ):
        self.search_word = search_word
        self.database_name = database_name
        self.distance_threshold = distance_threshold
        self.columns: list[WordIdentifierDistance] = []
        self.tables: list[WordIdentifierDistance] = []


    def __str__(self):
        string = f"---- VectorSeachResults ----\n  search_word: {self.search_word}\n  database_name: {self.database_name}"
        string += f"\n  distance_threshold: {str(self.distance_threshold)}\n  Tables:"
        for wid in self.tables:
            string += f"\n    {wid.database_identifier}: {str(wid.distance)}"
        string += "\n  Columns:"
        for wid in self.columns:
            string += f"\n    {wid.database_identifier}: {str(wid.distance)}"
        return string
    

