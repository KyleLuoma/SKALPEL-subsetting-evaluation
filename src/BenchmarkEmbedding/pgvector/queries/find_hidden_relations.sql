with matches (id_qn) 
as (
	select concat(schema_identifier, nlq.question_number) as id_qn
	from benchmark_gold_query_tables as gqt
	join benchmark_question_natural_language_question_word_embeddings nlq
	on nlq.embedding <=> gqt.embedding <= 0.25
		and nlq.benchmark_name = gqt.benchmark_name
		and nlq.database_name = gqt.database_name
		and nlq.question_number = gqt.question_number
	where nlq.benchmark_name = '__BENCHMARK_NAME__'
		and nlq.database_name = '__DATABASE_NAME__'
		and nlq.embedding_model = '__EMBEDDING_MODEL__'
    union 
	select concat(table_name, nlq.question_number) as id_qn
	from benchmark_gold_query_columns gqc
	join benchmark_question_natural_language_question_word_embeddings nlq
	on nlq.embedding <=> gqc.embedding <= 0.30
		and nlq.benchmark_name = gqc.benchmark_name
		and nlq.database_name = gqc.database_name
		and nlq.question_number = gqc.question_number
	where nlq.benchmark_name = '__BENCHMARK_NAME__'
		and nlq.database_name = '__DATABASE_NAME__'
		and nlq.embedding_model = '__EMBEDDING_MODEL__'
)
select benchmark_name, naturalness, database_name, question_number, schema_identifier 
from benchmark_gold_query_tables gqt
where concat(gqt.schema_identifier, gqt.question_number) not in (
	select id_qn from matches
)
	and gqt.benchmark_name = '__BENCHMARK_NAME__'
	and gqt.database_name = '__DATABASE_NAME__'
	and gqt.embedding_model = '__EMBEDDING_MODEL__'
    and gqt.naturalness = '__NATURALNESS__'
    and gqt.question_number = __QUESTION_NUMBER__
;