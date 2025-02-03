
from BenchmarkEmbedding.BenchmarkEmbedding import BenchmarkEmbedding
from BenchmarkEmbedding.VectorSearchResults import VectorSearchResults, WordIdentifierDistance
from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
import pandas as pd
import os
from tqdm import tqdm
from util.StringObjectParser import StringObjectParser as sop

def main():
    print(os.getcwd())
    results_folder = "./subsetting_results"
    results_filename = "subsetting-CodeS-snails-Native-NVIDIA_RTX_2000.xlsx"
    results_df = pd.read_excel(f"{results_folder}/{results_filename}")
    filename_params = results_filename.split("-")
    subsetter_name = filename_params[1]
    benchmark_name = filename_params[2]

    benchmark_factory = NlSqlBenchmarkFactory()
    benchmark = benchmark_factory.build_benchmark(benchmark_name=benchmark_name)

    benchmark_embedding = BenchmarkEmbedding(benchmark_name=benchmark_name)
    # benchmark_embedding.encode_benchmark(benchmark)
    # benchmark_embedding.encode_benchmark_questions(benchmark)
    benchmark_embedding.encode_benchmark_gold_query_identifiers(benchmark=benchmark)

    similarity_threshold = 0.7

    results_dict = {
        "database": [],
        "question_number": [],
        "hidden_relations": []
    }

    for row in tqdm(results_df.itertuples(), total=results_df.shape[0]):

        results_dict["database"].append(row.database)
        results_dict["question_number"].append(row.question_number)

        question_hidden_relations = set()

        if row.database != benchmark.get_active_schema().database:
            benchmark.set_active_schema(database_name=row.database)
        benchmark.set_active_question_number(number=row.question_number)
        question = benchmark.get_active_question()
        question_1grams = set(question.question.split(" "))

        correct_tables = sop.string_to_python_object(row.correct_tables)
        missing_tables = sop.string_to_python_object(row.missing_tables)
        gold_tables = correct_tables.union(missing_tables)
        
        correct_columns = sop.string_to_python_object(row.correct_columns)
        missing_columns = sop.string_to_python_object(row.missing_columns)
        gold_columns = correct_columns.union(missing_columns)

        table_word_distances = {}
        for table in gold_tables:
            word_dist_list = []
            for word in question_1grams:
                distance = benchmark_embedding.get_string_similarities(table, word)
                if distance >= similarity_threshold:
                    word_dist_list.append((word, distance))
            table_word_distances[table] = word_dist_list

        for table in table_word_distances:
            if len(table_word_distances[table]) == 0:
                question_hidden_relations.add(table)

        results_dict["hidden_relations"].append(question_hidden_relations)

        column_word_distances = {}
        for column in gold_columns:
            word_dist_list = []
            for word in question_1grams:
                distance = benchmark_embedding.get_string_similarities(column.split(".")[1], word)
                if distance >= similarity_threshold:
                    word_dist_list.append((word, distance))
            column_word_distances[column] = word_dist_list

        print(table_word_distances)
        print(column_word_distances)

    pd.DataFrame(results_dict).to_excel(
        f"{results_folder}/diagnosis/{results_filename.replace('subsetting-', 'subsetting-diagnosis-')}", 
        index=False
        )

if __name__ == "__main__":
    main()