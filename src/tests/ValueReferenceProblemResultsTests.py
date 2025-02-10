from BenchmarkEmbedding.ValueReferenceProblemResults import (
    ValueReferenceProblemItem, 
    ValueReferenceProblemResults
)

def item_eq_test():
    item1 = ValueReferenceProblemItem(
        table_name="a",
        column_name="b",
        db_text_value="c",
        nlq_ngram="d"
    )
    item2 = ValueReferenceProblemItem(
        table_name="a",
        column_name="b",
        db_text_value="c",
        nlq_ngram="d"
    )
    matched = item1 == item2
    item2.nlq_ngram = "x"
    not_matched = item1 == item2
    return matched and not not_matched



def item_get_item_test():
    item = ValueReferenceProblemItem(
        table_name="a",
        column_name="b",
        db_text_value="c",
        nlq_ngram="d"
    )
    return item["table_name"] == "a"


def item_str_test():
    item = ValueReferenceProblemItem(
        table_name="a",
        column_name="b",
        db_text_value="c",
        nlq_ngram="d"
    )
    return str(item) == str({
        "table_name":"a",
        "column_name":"b",
        "db_text_value":"c",
        "nlq_ngram":"d"
        })


def results_to_dict_test():
    item1 = ValueReferenceProblemItem(
        table_name="a",
        column_name="b",
        db_text_value="c",
        nlq_ngram="d"
    )
    item2 = ValueReferenceProblemItem(
        table_name="a",
        column_name="b",
        db_text_value="c",
        nlq_ngram="d"
    )
    results = ValueReferenceProblemResults(
        problem_columns=[item1, item2]
    )
    return results.to_dict() == {
        "table_name": ["a", "a"],
        "column_name": ["b", "b"],
        "text_value": ["c", "c"],
        "nlq_ngram": ["d", "d"]
    }

def results_get_unmatched_column_names_as_set_test():
    item1 = ValueReferenceProblemItem(
        table_name="a",
        column_name="x",
        db_text_value="c",
        nlq_ngram="d"
    )
    item2 = ValueReferenceProblemItem(
        table_name="a",
        column_name="y",
        db_text_value="c",
        nlq_ngram="d"
    )
    results = ValueReferenceProblemResults(
        problem_columns=[item1, item2]
    )
    unmatched_columns = results.get_unmatched_column_names_as_set()
    return unmatched_columns == {"a.x", "a.y"}

