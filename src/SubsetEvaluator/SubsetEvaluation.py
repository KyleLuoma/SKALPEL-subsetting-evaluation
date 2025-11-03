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

class SubsetEvaluation:


    def __init__(
            self,
            total_recall: float = None,
            total_precision: float = None,
            total_f1: float = None,
            table_recall: float = None,
            table_precision: float = None,
            table_f1: float = None,
            column_recall: float = None,
            column_precision: float = None,
            column_f1: float = None,
            correct_tables: set = None,
            missing_tables: set = None,
            extra_tables: set = None,
            correct_columns: set = None,
            missing_columns: set = None,
            extra_columns: set = None,
            subset_table_proportion: float = None,
            subset_column_proportion: float = None
            ):
        self.total_recall = total_recall
        self.total_precision = total_precision
        self.total_f1 = total_f1
        self.table_recall = table_recall
        self.table_precision = table_precision
        self.table_f1 = table_f1
        self.column_recall = column_recall
        self.column_precision = column_precision
        self.column_f1 = column_f1
        self.correct_tables = correct_tables
        self.missing_tables = missing_tables
        self.extra_tables = extra_tables
        self.correct_columns = correct_columns
        self.missing_columns = missing_columns
        self.extra_columns = extra_columns
        self.subset_table_proportion = subset_table_proportion
        self.subset_column_proportion = subset_column_proportion


    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return (
            self.total_recall == other.total_recall and
            self.total_precision == other.total_precision and
            self.total_f1 == other.total_f1 and
            self.table_recall == other.table_recall and
            self.table_precision == other.table_precision and
            self.table_f1 == other.table_f1 and
            self.column_recall == other.column_recall and
            self.column_precision == other.column_precision and
            self.column_f1 == other.column_f1 and
            self.correct_tables == other.correct_tables and
            self.missing_tables == other.missing_tables and
            self.extra_tables == other.extra_tables and
            self.correct_columns == other.correct_columns and
            self.missing_columns == other.missing_columns and
            self.extra_columns == other.extra_columns and
            self.subset_table_proportion == other.subset_table_proportion and
            self.subset_column_proportion == other.subset_column_proportion
        )
    

    def __getitem__(self, item_key):
        if item_key == "total_recall":
            return self.total_recall
        if item_key == "total_precision":
            return self.total_precision
        if item_key == "total_f1":
            return self.total_f1
        if item_key == "table_recall":
            return self.table_recall
        if item_key == "table_precision":
            return self.table_precision
        if item_key == "table_f1":
            return self.table_f1
        if item_key == "column_recall":
            return self.column_recall
        if item_key == "column_precision":
            return self.column_precision
        if item_key == "column_f1":
            return self.column_f1
        if item_key == "correct_tables":
            return self.correct_tables
        if item_key == "missing_tables":
            return self.missing_tables
        if item_key == "extra_tables":
            return self.extra_tables
        if item_key == "correct_columns":
            return self.correct_columns
        if item_key == "missing_columns":
            return self.missing_columns
        if item_key == "extra_columns":
            return self.extra_columns
        if item_key == "subset_table_proportion":
            return self.subset_table_proportion
        if item_key == "subset_column_proportion":
            return self.subset_column_proportion
        raise KeyError(item_key)
    

    def __str__(self):
        return str({
            "total_recall": self.total_recall,
            "total_precision": self.total_precision,
            "total_f1": self.total_f1,
            "table_recall": self.table_recall,
            "table_precision": self.table_precision,
            "table_f1": self.table_f1,
            "column_recall": self.column_recall,
            "column_precision": self.column_precision,
            "column_f1": self.column_f1,
            "correct_tables": self.correct_tables,
            "missing_tables": self.missing_tables,
            "extra_tables": self.extra_tables,
            "correct_columns": self.correct_columns,
            "missing_columns": self.missing_columns,
            "extra_columns": self.extra_columns,
            "subset_table_proportion": self.subset_table_proportion,
            "subset_column_proportion": self.subset_column_proportion
        })
    

    def keys(self):
        return [
            "total_recall",
            "total_precision",
            "total_f1",
            "table_recall",
            "table_precision",
            "table_f1",
            "column_recall",
            "column_precision",
            "column_f1",
            "correct_tables",
            "missing_tables",
            "extra_tables",
            "correct_columns",
            "missing_columns",
            "extra_columns",
            "subset_table_proportion",
            "subset_column_proportion"
        ]