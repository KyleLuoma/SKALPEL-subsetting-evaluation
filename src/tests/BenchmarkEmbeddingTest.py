from BenchmarkEmbedding.BenchmarkEmbedding import BenchmarkEmbedding
from BenchmarkEmbedding.ValueReferenceProblemResults import ValueReferenceProblemResults, ValueReferenceProblemItem
from BenchmarkEmbedding.VectorSearchResults import VectorSearchResults
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
import time


def init_test():
    bm_embed = BenchmarkEmbedding(benchmark_name="snails", verbose=True)
    return True


def get_embedding_test():
    bm_embed = BenchmarkEmbedding(benchmark_name="snails")
    print(bm_embed.get_embedding("customers_table"))
    return True


def get_similarity_test():
    bm_embed = BenchmarkEmbedding(benchmark_name="snails")
    emb_1 = bm_embed.get_embedding("customers")
    print(len(emb_1[0]))
    emb_2 = bm_embed.get_embedding("clients")
    sim_score1 = bm_embed.get_similarity(emb_1, emb_2)
    print(sim_score1)

    emb_1 = bm_embed.get_embedding("orange")
    emb_2 = bm_embed.get_embedding("computer")
    sim_score2 = bm_embed.get_similarity(emb_1, emb_2)
    print(sim_score2)
    return bool(sim_score1[0][0] > sim_score2[0][0])


def encode_benchmark_test():
    benchmark = NlSqlBenchmark()
    bm_embed = BenchmarkEmbedding(benchmark_name="abstract")
    cursor = bm_embed.db_conn.cursor()
    cursor.execute(
    """
    DELETE FROM database_table_word_embeddings
    WHERE benchmark_name = 'abstract'
    AND database_name = 'database1'
    AND schema_identifier = 'table1'
    AND embedding_model = %s
    """,
    [bm_embed.model_name]
    )
    cursor.execute(
    """
    DELETE FROM database_column_word_embeddings
    WHERE benchmark_name = 'abstract'
    AND database_name = 'database1'
    AND table_name = 'table1'
    AND schema_identifier = 'column1'
    AND embedding_model = %s
    """,
    [bm_embed.model_name]
    )
    encoded_count = bm_embed.encode_benchmark(benchmark=benchmark)
    print(encoded_count)
    return encoded_count[0] == 1 and encoded_count[1] == 1


def get_identifiers_from_semantic_search_test():
    bm_embed = BenchmarkEmbedding(benchmark_name="abstract")
    s_time = time.perf_counter()
    results = bm_embed.get_identifiers_from_semantic_search("table or column", "database1")
    end_time = time.perf_counter()
    print(results.tables[0], results.columns[0], "Retrieval time:", str(end_time - s_time))
    return (
        results.tables[0].database_identifier == "table1" 
        and results.columns[0].database_identifier == "table1.column1" 
        )


def get_value_reference_problem_results_test():
    bm_embed = BenchmarkEmbedding(benchmark_name="snails", instantiate_embedding_model=False)
    results = bm_embed.get_value_reference_problem_results(
        database_name="NTSB",
        question_number=33,
        naturalness="Native"
    )
    return len(results.problem_tables) == 14