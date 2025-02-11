from BenchmarkEmbedding.IdentifierAmbiguityProblemResults import (
    IdentifierAmbiguityProblemResults,
    IdentifierAmbiguityProblemItem    
    )
from NlSqlBenchmark.SchemaObjects import TableColumn, SchemaTable

def associate_column_with_word_nl_test():
    results = IdentifierAmbiguityProblemResults()
    attribute = TableColumn(name="column", data_type=None, table_name="table")
    results.associate_column_with_word_nl("test", attribute)
    return (
        results.word_nl_matches[0].word_nl == "test" 
        and attribute in results.word_nl_matches[0].matching_attributes
        )


def associate_table_with_word_nl_test():
    results = IdentifierAmbiguityProblemResults()
    relation = SchemaTable(name="table", columns=[], primary_keys=[], foreign_keys=[])
    results.associate_table_with_word_nl("test", relation)
    return (
        results.word_nl_matches[0].word_nl == "test"
        and relation in results.word_nl_matches[0].matching_relations
    )


def associate_two_tables_with_word_nl_test():
    results = IdentifierAmbiguityProblemResults()
    relation1 = SchemaTable(name="table1", columns=[], primary_keys=[], foreign_keys=[])
    relation2 = SchemaTable(name="table2", columns=[], primary_keys=[], foreign_keys=[])
    results.associate_table_with_word_nl("test", relation1)
    results.associate_table_with_word_nl("test", relation2)
    return (
        results.word_nl_matches[0].word_nl == "test"
        and relation1 in results.word_nl_matches[0].matching_relations
        and relation2 in results.word_nl_matches[0].matching_relations
    )


def associate_two_tables_two_columns_with_word_nl_test():
    results = IdentifierAmbiguityProblemResults()
    relation1 = SchemaTable(name="table1", columns=[], primary_keys=[], foreign_keys=[])
    relation2 = SchemaTable(name="table2", columns=[], primary_keys=[], foreign_keys=[])
    attribute1 = TableColumn(name="column1", data_type=None, table_name="table1")
    attribute2 = TableColumn(name="column2", data_type=None, table_name="table2")
    results.associate_table_with_word_nl("test", relation1)
    results.associate_table_with_word_nl("test", relation2)
    results.associate_column_with_word_nl("test", attribute1)
    results.associate_column_with_word_nl("test", attribute2)
    return (
        results.word_nl_matches[0].word_nl == "test"
        and relation1 in results.word_nl_matches[0].matching_relations
        and relation2 in results.word_nl_matches[0].matching_relations
        and attribute1 in results.word_nl_matches[0].matching_attributes
        and attribute2 in results.word_nl_matches[0].matching_attributes
    )


def get_all_ambiguous_tables_test():
    results = IdentifierAmbiguityProblemResults()
    relation1 = SchemaTable(name="table1", columns=[], primary_keys=[], foreign_keys=[])
    relation2 = SchemaTable(name="table2", columns=[], primary_keys=[], foreign_keys=[])
    results.associate_table_with_word_nl("test", relation1)
    results.associate_table_with_word_nl("test", relation2)
    tables = results.get_all_ambiguous_tables()
    return (
        tables == {relation1, relation2}
    )


def get_all_ambiguous_columns_test():
    results = IdentifierAmbiguityProblemResults()
    attribute1 = TableColumn(name="column1", data_type=None, table_name="table1")
    attribute2 = TableColumn(name="column2", data_type=None, table_name="table2")
    results.associate_column_with_word_nl("test", attribute1)
    results.associate_column_with_word_nl("test", attribute2)
    columns = results.get_all_ambiguous_columns()
    return (
        columns == {attribute1, attribute2}
    )


def get_all_word_nl_test():
    results = IdentifierAmbiguityProblemResults()
    relation1 = SchemaTable(name="table1", columns=[], primary_keys=[], foreign_keys=[])
    relation2 = SchemaTable(name="table2", columns=[], primary_keys=[], foreign_keys=[])
    results.associate_table_with_word_nl(word_nl="test1", table=relation1)
    results.associate_table_with_word_nl(word_nl="test2", table=relation2)
    return (
        results.get_all_word_nl() == {"test1", "test2"}
    )



def results_eq_test():
    correct_result1 = IdentifierAmbiguityProblemResults(
        word_nl_matches=[
            IdentifierAmbiguityProblemItem(
                word_nl="crashes",
                matching_relations={
                    SchemaTable(name="CRASH"),
                    SchemaTable(name="EDRPOSTCRASH"),
                    SchemaTable(name="EDRPRECRASH"),
                },
                matching_attributes={
                    TableColumn(name="CrashImminentBraking", table_name="VPICDECODE"),
                    TableColumn(name="PREVCRASH", table_name="AIRBAG"),
                    TableColumn(name="IGCYCRASH", table_name="EDREVENT")
                }
            )
        ]
    )
    correct_result2 = IdentifierAmbiguityProblemResults(
        word_nl_matches=[
            IdentifierAmbiguityProblemItem(
                word_nl="crashes",
                matching_relations={
                    SchemaTable(name="CRASH"),
                    SchemaTable(name="EDRPOSTCRASH"),
                    SchemaTable(name="EDRPRECRASH"),
                },
                matching_attributes={
                    TableColumn(name="CrashImminentBraking", table_name="VPICDECODE"),
                    TableColumn(name="PREVCRASH", table_name="AIRBAG"),
                    TableColumn(name="IGCYCRASH", table_name="EDREVENT")
                }
            )
        ]
    )
    return correct_result1 == correct_result2


def results_not_eq_test():
    correct_result1 = IdentifierAmbiguityProblemResults(
        word_nl_matches=[
            IdentifierAmbiguityProblemItem(
                word_nl="crashes",
                matching_relations={
                    SchemaTable(name="CRASH"),
                    SchemaTable(name="EDRPOSTCRASH"),
                    SchemaTable(name="EDRPRECRASH"),
                },
                matching_attributes={
                    TableColumn(name="CrashImminentBraking", table_name="VPICDECODE"),
                    TableColumn(name="PREVCRASH", table_name="AIRBAG"),
                    TableColumn(name="IGCYCRASH", table_name="EDREVENT")
                }
            )
        ]
    )
    correct_result2 = IdentifierAmbiguityProblemResults(
        word_nl_matches=[
            IdentifierAmbiguityProblemItem(
                word_nl="crashes",
                matching_relations={
                    SchemaTable(name="CRASH"),
                    SchemaTable(name="EDRPOSTCRASH"),
                },
                matching_attributes={
                    TableColumn(name="CrashImminentBraking", table_name="VPICDECODE"),
                    TableColumn(name="PREVCRASH", table_name="AIRBAG"),
                    TableColumn(name="IGCYCRASH", table_name="EDREVENT")
                }
            )
        ]
    )
    return correct_result1 != correct_result2