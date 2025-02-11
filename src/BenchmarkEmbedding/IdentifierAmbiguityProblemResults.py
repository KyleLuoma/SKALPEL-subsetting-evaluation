from NlSqlBenchmark.SchemaObjects import SchemaTable, TableColumn

class IdentifierAmbiguityProblemItem:

    def __init__(
            self,
            word_nl: str,
            matching_relations: set[SchemaTable],
            matching_attributes: set[TableColumn]
            ):
        self.word_nl = word_nl
        self.matching_relations = matching_relations
        self.matching_attributes = matching_attributes



    def __str__(self):
        return str({
            "word_nl": self.word_nl,
            "matching_relations": self.matching_relations,
            "matching_attributes": self.matching_attributes
        })



    def __getitem__(self, item_key):
        if item_key == "word_nl":
            return self.word_nl
        if item_key == "matching_relations":
            return self.matching_relations
        if item_key == "matching_attributes":
            return self.matching_attributes
        raise KeyError(f"Key {item_key} not found in IdentifierAmbiguityProblemItem")
    


    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return (
            self.word_nl == other.word_nl and
            self.matching_relations == other.matching_relations and
            self.matching_attributes == other.matching_attributes
        )



class IdentifierAmbiguityProblemResults:

    def __init__(
            self,
            word_nl_matches: list[IdentifierAmbiguityProblemItem] = None
            ):
        if word_nl_matches != None:
            self.word_nl_matches = word_nl_matches
        else: 
            self.word_nl_matches = []



    def __eq__(self, other) -> bool:
        if len(self.word_nl_matches) != len(other.word_nl_matches):
            return False
        for match in self.word_nl_matches:
            if match not in other.word_nl_matches:
                return False
        return True



    def add_item(self, item: IdentifierAmbiguityProblemItem) -> None:
        self.word_nl_matches.append(item)
        return
    


    def get_all_word_nl(self) -> set[str]:
        all_word_nl = set()
        for item in self.word_nl_matches:
            all_word_nl.add(item.word_nl)
        return all_word_nl
    


    def get_all_ambiguous_tables(self) -> set[SchemaTable]:
        ambiguous_tables = set()
        for item in self.word_nl_matches:
            ambiguous_tables = ambiguous_tables.union(item.matching_relations)
        return ambiguous_tables
    


    def get_all_ambiguous_columns(self) -> set[TableColumn]:
        ambiguous_columns = set()
        for item in self.word_nl_matches:
            ambiguous_columns = ambiguous_columns.union(item.matching_attributes)
        return ambiguous_columns
    


    def associate_column_with_word_nl(
            self, 
            word_nl: str, 
            column: TableColumn
            ) -> None:
        if word_nl not in self.get_all_word_nl():
            new_item = IdentifierAmbiguityProblemItem(
                word_nl=word_nl,
                matching_relations=set(),
                matching_attributes={column}
            )
            self.add_item(new_item)
            return
        for item in self.word_nl_matches:
            if item.word_nl != word_nl:
                continue
            item.matching_attributes.add(column)
        return



    def associate_table_with_word_nl(
            self,
            word_nl: str,
            table: SchemaTable
            ) -> None:
        if word_nl not in self.get_all_word_nl():
            new_item = IdentifierAmbiguityProblemItem(
                word_nl=word_nl,
                matching_relations={table},
                matching_attributes=set()
            )
            self.add_item(new_item)
            return
        for item in self.word_nl_matches:
            if item.word_nl != word_nl:
                continue
            item.matching_relations.add(table)
        return