from BenchmarkEmbedding.BenchmarkEmbedding import BenchmarkEmbedding



def init_test():
    bm_embed = BenchmarkEmbedding(benchmark_name="snails", verbose=True)
    return True


# def get_embedding_test():
#     bm_embed = BenchmarkEmbedding(benchmark_name="snails")
#     print(bm_embed.get_embedding("customers_table"))
#     return True


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