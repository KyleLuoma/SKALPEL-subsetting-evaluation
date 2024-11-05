from SchemaSubsetter.CodeSSubsetter import CodeSSubsetter
from NlSqlBenchmark.BirdNlSqlBenchmark import BirdNlSqlBenchmark

def adapt_benchmark_schema_test():
    benchmark = BirdNlSqlBenchmark()
    subsetter = CodeSSubsetter(benchmark)
    adapted_schema = subsetter.adapt_benchmark_schema(benchmark.get_active_schema(), question="test question")
    return adapted_schema[0]["schema"]["schema_items"][0] == {
        'table_name': 'customers', 
        'table_comment': '', 
        'column_names': ['CustomerID', 'Segment', 'Currency'], 
        'column_types': ['integer', 'text', 'text'], 
        'column_comments': ['', '', ''], 
        'column_contents': [[3, 5], ['SME', 'LAM'], ['EUR', 'EUR']], 
        'pk_indicators': [1, 0, 0]
        }



def filter_schema_test():
    benchmark = BirdNlSqlBenchmark()
    subsetter = CodeSSubsetter(benchmark)
    full_dataset = subsetter.adapt_benchmark_schema(
        schema=benchmark.get_active_schema(), 
        question=benchmark.get_active_question()["question"]
        )
    all_tables = set([t["table_name"] for t in full_dataset[0]["schema"]["schema_items"]])
    filtered = subsetter.filter_schema(
        dataset=full_dataset,
        dataset_type="eval",
        sic=subsetter.sic
        )
    filtered_tables = set([t["table_name"] for t in filtered[0]["schema"]["schema_items"]])
    return filtered_tables.issubset(all_tables)
    


def get_schema_subset_test():
    benchmark = BirdNlSqlBenchmark()
    subsetter = CodeSSubsetter(benchmark)
    subset = subsetter.get_schema_subset(
        question=benchmark.get_active_question()["question"],
        full_schema=benchmark.get_active_schema()
    )
    return "tables" in subset.keys()