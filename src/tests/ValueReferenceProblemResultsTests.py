from BenchmarkEmbedding.ValueReferenceProblemResults import (
    ValueReferenceProblemItem, 
    ValueReferenceProblemResults
)

def item_eq_test():
    item1 = ValueReferenceProblemItem(
        item_matched="table",
        table_name="a",
        column_name="b",
        db_text_value="c",
        nlq_ngram="d",
        nlq_ngram_db_text_value_distance=0.5,
        item_name_matched_nlq_ngram=True
    )
    item2 = ValueReferenceProblemItem(
        item_matched="table",
        table_name="a",
        column_name="b",
        db_text_value="c",
        nlq_ngram="d",
        nlq_ngram_db_text_value_distance=0.5,
        item_name_matched_nlq_ngram=True
    )
    matched = item1 == item2
    item2.nlq_ngram = "x"
    not_matched = item1 == item2
    return matched and not not_matched



def item_get_item_test():
    item = ValueReferenceProblemItem(
        item_matched="table",
        table_name="a",
        column_name="b",
        db_text_value="c",
        nlq_ngram="d",
        nlq_ngram_db_text_value_distance=0.5,
        item_name_matched_nlq_ngram=True
    )
    return item["table_name"] == "a"


def item_str_test():
    item = ValueReferenceProblemItem(
        item_matched="table",
        table_name="a",
        column_name="b",
        db_text_value="c",
        nlq_ngram="d",
        nlq_ngram_db_text_value_distance=0.5,
        item_name_matched_nlq_ngram=True
    )
    return str(item) == str({
        "item_matched":"table",
        "table_name":"a",
        "column_name":"b",
        "db_text_value":"c",
        "nlq_ngram":"d",
        "nlq_ngram_db_text_value_distance":0.5,
        "item_name_matched_nlq_ngram":True
        })


def results_to_dict_test():
    item1 = ValueReferenceProblemItem(
        item_matched="table",
        table_name="a",
        column_name="b",
        db_text_value="c",
        nlq_ngram="d",
        nlq_ngram_db_text_value_distance=0.5,
        item_name_matched_nlq_ngram=True
    )
    item2 = ValueReferenceProblemItem(
        item_matched="column",
        table_name="a",
        column_name="b",
        db_text_value="c",
        nlq_ngram="d",
        nlq_ngram_db_text_value_distance=0.5,
        item_name_matched_nlq_ngram=True
    )
    results = ValueReferenceProblemResults(
        problem_tables=[item1],
        problem_columns=[item2]
    )
    return results.to_dict() == {
        "item_matched": ["table", "column"],
        "table_name": ["a", "a"],
        "column_name": ["b", "b"],
        "text_value": ["c", "c"],
        "nlq_ngram": ["d", "d"],
        "nlq_ngram_db_text_value_distance": [0.5, 0.5],
        "item_name_matched_nlq_ngram": [True, True]
    }

def results_get_unmatched_column_names_as_set_test():
    item1 = ValueReferenceProblemItem(
        item_matched="column",
        table_name="a",
        column_name="x",
        db_text_value="c",
        nlq_ngram="d",
        nlq_ngram_db_text_value_distance=0.5,
        item_name_matched_nlq_ngram=True
    )
    item2 = ValueReferenceProblemItem(
        item_matched="column",
        table_name="a",
        column_name="y",
        db_text_value="c",
        nlq_ngram="d",
        nlq_ngram_db_text_value_distance=0.5,
        item_name_matched_nlq_ngram=False
    )
    results = ValueReferenceProblemResults(
        problem_tables=[],
        problem_columns=[item1, item2]
    )
    unmatched_columns = results.get_unmatched_column_names_as_set()
    return unmatched_columns == {"a.y"}

def results_get_unmatched_table_names_as_set_test():
    item1 = ValueReferenceProblemItem(
        item_matched="table",
        table_name="a",
        column_name="x",
        db_text_value="c",
        nlq_ngram="d",
        nlq_ngram_db_text_value_distance=0.5,
        item_name_matched_nlq_ngram=True
    )
    item2 = ValueReferenceProblemItem(
        item_matched="table",
        table_name="b",
        column_name="y",
        db_text_value="c",
        nlq_ngram="d",
        nlq_ngram_db_text_value_distance=0.5,
        item_name_matched_nlq_ngram=False
    )
    results = ValueReferenceProblemResults(
        problem_tables=[item1, item2],
        problem_columns=[]
    )
    unmatched_tables = results.get_unmatched_table_names_as_set()
    return unmatched_tables == {"b"}