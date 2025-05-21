
from BenchmarkEmbedding.BenchmarkEmbedding import BenchmarkEmbedding
from BenchmarkEmbedding.VectorSearchResults import VectorSearchResults, WordIdentifierDistance
from BenchmarkEmbedding.IdentifierAmbiguityProblemResults import (
    IdentifierAmbiguityProblemResults,
    IdentifierAmbiguityProblemItem
)
from NlSqlBenchmark.SchemaObjects import (
    SchemaTable,
    TableColumn
)
from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
import pandas as pd
import os
from tqdm import tqdm
from util.StringObjectParser import StringObjectParser as sop


def main():
    results_folder = "./subsetting_results"
    diagnosis_folder = f"{results_folder}/diagnosis"

    if not os.path.exists(diagnosis_folder):
        os.makedirs(diagnosis_folder)

    # diagnose_subsets(f"subsetting-rslsql-bird-Native-gpt4o.xlsx")
    # return
    for filename in os.listdir(results_folder):
        if filename.endswith(".xlsx") and not filename.startswith("subsetting-diagnosis-"):
            diagnosis_file = f"{diagnosis_folder}/{filename.replace('subsetting-', 'subsetting-diagnosis-')}"
            if not os.path.exists(diagnosis_file):
                diagnose_subsets(filename)


def diagnose_subsets(
        results_filename: str        
):
    print(os.getcwd())
    naturalness = "Native"
    results_folder = "./subsetting_results"
    # results_filename = f"subsetting-chess-bird-{naturalness}-gpt4o.xlsx"
    results_df = pd.read_excel(f"{results_folder}/{results_filename}")
    filename_params = results_filename.split("-")
    subsetter_name = filename_params[1]
    benchmark_name = filename_params[2]

    benchmark_factory = NlSqlBenchmarkFactory()
    benchmark = benchmark_factory.build_benchmark(benchmark_name=benchmark_name)

    benchmark_embedding = BenchmarkEmbedding(
        benchmark_name=benchmark_name, 
        build_database_on_init=True,
        db_host_profile="remote",
        db_host="cdas2"
        )

    ### Benchmark encoding tasks, uncomment as-needed ###

    # benchmark_embedding.encode_benchmark(benchmark)
    # benchmark_embedding.encode_benchmark_questions(benchmark)
    # benchmark_embedding.encode_benchmark_gold_query_identifiers(benchmark=benchmark)
    # benchmark_embedding.encode_benchmark_values(benchmark=benchmark) #All values no longer needed
    # benchmark_embedding.encode_benchmark_gold_query_predicates(benchmark=benchmark)
    # return

    results_dict = {
        "database": [],
        "question_number": [],
        "gold_query_tables": [],
        "missing_tables": [],
        "hidden_tables": [],
        "ambiguous_extra_tables": [],
        "gold_query_columns": [],
        "missing_columns": [],
        "value_reference_problem_columns": [],
        "ambiguous_extra_columns": []
    }
    value_reference_problem_results_dict = {
        "table_name": [],
        "column_name": [],
        "text_value": [],
        "nlq_ngram": []
    }

    for row in tqdm(results_df.itertuples(), total=results_df.shape[0], desc=f"Diagnosing {results_filename}"):

        correct_tables = set(sop.string_to_python_object(row.correct_tables, use_eval=True))
        missing_tables = set(sop.string_to_python_object(row.missing_tables, use_eval=True))
        results_dict["gold_query_tables"].append(
            correct_tables.union(missing_tables)
        )

        results_dict["database"].append(row.database)
        results_dict["question_number"].append(row.question_number)
        if len(missing_tables) == 0:
            results_dict["missing_tables"].append("{}")
        else:
            results_dict["missing_tables"].append(missing_tables)

        correct_attributes = set(sop.string_to_python_object(row.correct_columns, use_eval=True))
        missing_attributes = set(sop.string_to_python_object(row.missing_columns, use_eval=True))
        results_dict["gold_query_columns"].append(
            correct_attributes.union(missing_attributes)
        )
        if len(missing_attributes) == 0:
            results_dict["missing_columns"].append("{}")
        else:
            results_dict["missing_columns"].append(missing_attributes)

        ## Discover hidden relation problems ##
        hidden_relations = benchmark_embedding.get_hidden_relations(
            database_name=row.database,
            question_number=row.question_number,
            naturalness=naturalness
        ).intersection(missing_tables)
        if len(hidden_relations) == 0:
            results_dict["hidden_tables"].append("{}")
        else:
            results_dict["hidden_tables"].append(hidden_relations)

        ## Discover value reference problems ##
        value_reference_problems = benchmark_embedding.get_value_reference_problem_results(
            database_name=row.database,
            question_number=row.question_number,
            naturalness=naturalness
        )
        question_vrp_items_dict = value_reference_problems.to_dict()
        for column in question_vrp_items_dict:
            value_reference_problem_results_dict[column] += question_vrp_items_dict[column]

        unmatched_attributes = {}
        missing_columns_upper = [c.upper() for c in missing_attributes]
        for col in value_reference_problems.problem_columns:
            if f"{col.table_name.upper()}.{col.column_name.upper()}" not in missing_columns_upper:
                continue
            if f"{col.table_name}.{col.column_name}" not in unmatched_attributes.keys():
                unmatched_attributes[f"{col.table_name}.{col.column_name}"] = [(col.nlq_ngram, col.db_text_value)]
            else:
                unmatched_attributes[f"{col.table_name}.{col.column_name}"].append((col.nlq_ngram, col.db_text_value))

        if len(unmatched_attributes) > 0:
            results_dict["value_reference_problem_columns"].append(unmatched_attributes)
        else:
            results_dict["value_reference_problem_columns"].append("{}")


        ## Discover word NL ~ Identifier ambiguity problems ##
        ambiguous_items = benchmark_embedding.get_identifier_ambiguity_problem_results(
            database_name=row.database,
            question_number=row.question_number,
            naturalness=naturalness
        )
        
        extra_tables = set(sop.string_to_python_object(row.extra_tables if row.extra_tables != "set()" else "{}", use_eval=True))
        extra_columns = set(sop.string_to_python_object(row.extra_columns if row.extra_columns != "set()" else "{}", use_eval=True))

        ambiguous_columns = {}
        ambiguous_tables = {}

        for item in ambiguous_items.word_nl_matches:
            matching_relations = set([r.name for r in item.matching_relations])
            matching_attributes = set([a.name_as_string() for a in item.matching_attributes])
            if len(extra_columns.intersection(matching_attributes)) > 0:
                ambiguous_columns[item.word_nl] = extra_columns.intersection(matching_attributes)
            if len(extra_tables.intersection(matching_relations)) > 0:
                ambiguous_tables[item.word_nl] = extra_tables.intersection(matching_relations)

        results_dict["ambiguous_extra_columns"].append(ambiguous_columns)

        results_dict["ambiguous_extra_tables"].append(ambiguous_tables)

    pd.DataFrame(results_dict).to_excel(
        f"{results_folder}/diagnosis/{results_filename.replace('subsetting-', 'subsetting-diagnosis-')}", 
        index=False
        )
    pd.DataFrame(value_reference_problem_results_dict).to_excel(
        f"{results_folder}/diagnosis/value_reference_problems_expanded/{results_filename.replace('subsetting-', 'value-reference-problem-diagnosis-lambdatest')}",
        index=False
    )

if __name__ == "__main__":
    main()