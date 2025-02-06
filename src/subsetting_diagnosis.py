
from BenchmarkEmbedding.BenchmarkEmbedding import BenchmarkEmbedding
from BenchmarkEmbedding.VectorSearchResults import VectorSearchResults, WordIdentifierDistance
from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
import pandas as pd
import os
from tqdm import tqdm
from util.StringObjectParser import StringObjectParser as sop

def main():
    print(os.getcwd())
    naturalness = "Native"
    results_folder = "./subsetting_results"
    results_filename = f"subsetting-CodeS-snails-{naturalness}-NVIDIA_RTX_2000.xlsx"
    results_df = pd.read_excel(f"{results_folder}/{results_filename}")
    filename_params = results_filename.split("-")
    subsetter_name = filename_params[1]
    benchmark_name = filename_params[2]

    benchmark_factory = NlSqlBenchmarkFactory()
    benchmark = benchmark_factory.build_benchmark(benchmark_name=benchmark_name)

    benchmark_embedding = BenchmarkEmbedding(benchmark_name=benchmark_name)

    ### Benchmark encoding tasks, uncomment as-needed ###

    # benchmark_embedding.encode_benchmark(benchmark)
    # benchmark_embedding.encode_benchmark_questions(benchmark)
    # benchmark_embedding.encode_benchmark_gold_query_identifiers(benchmark=benchmark)
    # benchmark_embedding.encode_benchmark_values(benchmark=benchmark)
    # return

    results_dict = {
        "database": [],
        "question_number": [],
        "gold_query_tables": [],
        "missing_tables": [],
        "hidden_relations": [],
        "value_reference_problem_relations": [],
        "gold_query_attributes": [],
        "missing_attributes": [],
        "value_reference_problem_attributes": []
    }
    value_reference_problem_results_dict = {
        "item_matched": [],
        "table_name": [],
        "column_name": [],
        "text_value": [],
        "nlq_ngram": [],
        "nlq_ngram_db_text_value_distance": [],
        "item_name_matched_nlq_ngram": []
    }

    break_count = 0
    for row in tqdm(results_df.itertuples(), total=results_df.shape[0]):

        correct_tables = set(sop.string_to_python_object(row.correct_tables))
        missing_tables = set(sop.string_to_python_object(row.missing_tables))
        results_dict["gold_query_tables"].append(
            correct_tables.union(missing_tables)
        )

        results_dict["database"].append(row.database)
        results_dict["question_number"].append(row.question_number)
        if len(missing_tables) == 0:
            results_dict["missing_tables"].append("{}")
        else:
            results_dict["missing_tables"].append(missing_tables)
        
        correct_attributes = set(sop.string_to_python_object(row.correct_columns))
        missing_attributes = set(sop.string_to_python_object(row.missing_columns))
        results_dict["gold_query_attributes"].append(
            correct_attributes.union(missing_attributes)
        )
        if len(missing_attributes) == 0:
            results_dict["missing_attributes"].append("{}")
        else:
            results_dict["missing_attributes"].append(missing_attributes)

        ## Discover hidden relation problems ##
        hidden_relations = benchmark_embedding.get_hidden_relations(
            database_name=row.database,
            question_number=row.question_number,
            naturalness=naturalness
        ).intersection(missing_tables)
        if len(hidden_relations) == 0:
            results_dict["hidden_relations"].append("{}")
        else:
            results_dict["hidden_relations"].append(hidden_relations)

        ## Discover value reference problems ##
        value_reference_problems = benchmark_embedding.get_value_reference_problem_results(
            database_name=row.database,
            question_number=row.question_number,
            naturalness=naturalness
        )
        question_vrp_items_dict = value_reference_problems.to_dict()
        for column in question_vrp_items_dict:
            value_reference_problem_results_dict[column] += question_vrp_items_dict[column]

        unmatched_tables = value_reference_problems.get_unmatched_table_names_as_set()
        unmatched_tables = unmatched_tables - correct_tables
        if len(unmatched_tables) > 0:
            results_dict["value_reference_problem_relations"].append(unmatched_tables)
        else:
            results_dict["value_reference_problem_relations"].append("{}")

        unmatched_attributes = value_reference_problems.get_unmatched_column_names_as_set()
        unmatched_attributes = unmatched_attributes - correct_attributes
        if len(unmatched_attributes) > 0:
            results_dict["value_reference_problem_attributes"].append(unmatched_attributes)
        else:
            results_dict["value_reference_problem_attributes"].append("{}")
        if break_count > 100:
            break
        else:
            break_count += 1
        

    pd.DataFrame(results_dict).to_excel(
        f"{results_folder}/diagnosis/{results_filename.replace('subsetting-', 'subsetting-diagnosis-')}", 
        index=False
        )
    pd.DataFrame(value_reference_problem_results_dict).to_excel(
        f"{results_folder}/diagnosis/value_reference_problems_expanded/{results_filename.replace('subsetting-', 'value-reference-problem-diagnosis-')}",
        index=False
    )

if __name__ == "__main__":
    main()