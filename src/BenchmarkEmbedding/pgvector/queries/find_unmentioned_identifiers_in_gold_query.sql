-- Retrieves all schema identifiers in a specified gold query and joins ngrams from the associated NLQ
-- that are closer than (or equal to) a cosine similarity distance of 0.35.
select gqt.schema_identifier, nlq.ngram, n, nlq.embedding <=> gqt.embedding as distance, 'table' as table_or_column
from benchmark_question_natural_language_question_word_embeddings nlq
right join benchmark_gold_query_tables gqt 
	on nlq.embedding <=> gqt.embedding <= 0.35
	and nlq.question_number = gqt.question_number
	and nlq.database_name = gqt.database_name 
where (gqt.database_name = __DATABASE_NAME__ or nlq.database_name = __DATABASE_NAME__) 
    and (nlq.benchmark_name = __BENCHMARK_NAME__ or gqt.benchmark_name = __BENCHMARK_NAME__)
	and (gqt.question_number = __QUESTION_NUMBER__ or nlq.question_number = __QUESTION_NUMBER__)
	and gqt.naturalness = __NATURALNESS__
union 
select gqc.schema_identifier, nlq2.ngram, n, nlq2.embedding <=> gqc.embedding as distance, 'column' as table_or_column
from benchmark_question_natural_language_question_word_embeddings nlq2
right join benchmark_gold_query_columns gqc
	on nlq2.embedding <=> gqc.embedding <= 0.35
	and nlq2.question_number = gqc.question_number
	and nlq2.database_name = gqc.database_name
where (nlq2.database_name = __DATABASE_NAME__ or gqc.database_name = __DATABASE_NAME__)
    and (nlq2.benchmark_name = __BENCHMARK_NAME__ or gqc.benchmark_name = __BENCHMARK_NAME__)
	and (nlq2.question_number = __QUESTION_NUMBER__ or gqc.question_number = __QUESTION_NUMBER__)
    and gqc.naturalness = __NATURALNESS__
;
