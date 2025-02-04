
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
    # benchmark_embedding.encode_benchmark(benchmark)
    # benchmark_embedding.encode_benchmark_questions(benchmark)
    # benchmark_embedding.encode_benchmark_gold_query_identifiers(benchmark=benchmark)
    benchmark_embedding.encode_benchmark_values(benchmark=benchmark)
    return

    similarity_threshold = 0.7

    results_dict = {
        "database": [],
        "question_number": [],
        "gold_query_tables": [],
        "missing_tables": [],
        "hidden_relations": []
    }

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
        hidden_relations = benchmark_embedding.get_hidden_relations(
            database_name=row.database,
            question_number=row.question_number,
            naturalness=naturalness
        ).intersection(missing_tables)
        if len(hidden_relations) == 0:
            results_dict["hidden_relations"].append("{}")
        else:
            results_dict["hidden_relations"].append(hidden_relations)
        

    pd.DataFrame(results_dict).to_excel(
        f"{results_folder}/diagnosis/{results_filename.replace('subsetting-', 'subsetting-diagnosis-')}", 
        index=False
        )

if __name__ == "__main__":
    main()