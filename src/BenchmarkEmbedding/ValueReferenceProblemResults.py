
class ValueReferenceProblemItem:

    def __init__(
            self,
            item_matched: str, #'table' or 'column'
            table_name: str,
            column_name: str,
            db_text_value: str,
            nlq_ngram: str,
            nlq_ngram_db_text_value_distance: float,
            item_name_matched_nlq_ngram: bool
            ):
        assert item_matched in ("table", "column")
        self.item_matched = item_matched
        self.table_name = table_name
        self.column_name = column_name
        self.db_text_value = db_text_value
        self.nlq_ngram = nlq_ngram
        self.nlq_ngram_db_text_value_distance = nlq_ngram_db_text_value_distance
        self.item_name_matched_nlq_ngram = item_name_matched_nlq_ngram

    
    def __str__(self):
        return str({
            "item_matched": self.item_matched,
            "table_name": self.table_name,
            "column_name": self.column_name,
            "db_text_value": self.db_text_value,
            "nlq_ngram": self.nlq_ngram,
            "nlq_ngram_db_text_value_distance": self.nlq_ngram_db_text_value_distance,
            "item_name_matched_nlq_ngram": self.item_name_matched_nlq_ngram
        })
    

    def __getitem__(self, item_key):
        if item_key == "item_matched":
            return self.item_matched
        if item_key == "table_name":
            return self.table_name
        if item_key == "column_name":
            return self.column_name
        if item_key == "db_text_value":
            return self.db_text_value
        if item_key == "nlq_ngram":
            return self.nlq_ngram
        if item_key == "nlq_ngram_db_text_value_distance":
            return self.nlq_ngram_db_text_value_distance
        if item_key == "item_name_matched_nlq_ngram":
            return self.item_name_matched_nlq_ngram
        raise KeyError(f"Key {item_key} not found in ValueReferenceProblemTable")
    

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return (
            self.item_matched == other.item_matched
            and self.table_name == other.table_name
            and self.column_name == other.column_name
            and self.db_text_value == other.db_text_value
            and self.nlq_ngram == other.nlq_ngram
            and self.nlq_ngram_db_text_value_distance == other.nlq_ngram_db_text_value_distance
            and self.item_name_matched_nlq_ngram == other.item_name_matched_nlq_ngram
        )
    


class ValueReferenceProblemResults:

    def __init__(
            self, 
            problem_tables: list[ValueReferenceProblemItem],
            problem_columns: list[ValueReferenceProblemItem]
            ):
        self.problem_tables = problem_tables
        self.problem_columns = problem_columns


    def get_unmatched_table_names_as_set(self) -> set:
        table_names = set()
        for table in self.problem_tables:
            if not table.item_name_matched_nlq_ngram:
                table_names.add(table.table_name)
        return table_names
    

    def get_unmatched_column_names_as_set(self) -> set:
        column_names = set()
        for column in self.problem_columns:
            if not column.item_name_matched_nlq_ngram:
                column_names.add(f"{column.table_name}.{column.column_name}")
        return column_names


    def to_dict(self):
        return_dict = {
            "item_matched": [],
            "table_name": [],
            "column_name": [],
            "text_value": [],
            "nlq_ngram": [],
            "nlq_ngram_db_text_value_distance": [],
            "item_name_matched_nlq_ngram": []
        }
        for table in self.problem_tables:
            return_dict["item_matched"].append(table.item_matched)
            return_dict["table_name"].append(table.table_name)
            return_dict["column_name"].append(table.column_name)
            return_dict["text_value"].append(table.db_text_value)
            return_dict["nlq_ngram"].append(table.nlq_ngram)
            return_dict["nlq_ngram_db_text_value_distance"].append(table.nlq_ngram_db_text_value_distance)
            return_dict["item_name_matched_nlq_ngram"].append(table.item_name_matched_nlq_ngram)
        for column in self.problem_columns:
            return_dict["item_matched"].append(column.item_matched)
            return_dict["table_name"].append(column.table_name)
            return_dict["column_name"].append(column.column_name)
            return_dict["text_value"].append(column.db_text_value)
            return_dict["nlq_ngram"].append(column.nlq_ngram)
            return_dict["nlq_ngram_db_text_value_distance"].append(column.nlq_ngram_db_text_value_distance)
            return_dict["item_name_matched_nlq_ngram"].append(column.item_name_matched_nlq_ngram)
        return return_dict

