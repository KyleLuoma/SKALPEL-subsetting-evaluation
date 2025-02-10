with word_relation_matches as (
select ngram, schema_identifier, qnle.embedding <=> dtwe.embedding as distance
from benchmark_question_natural_language_question_word_embeddings qnle 
join database_table_word_embeddings dtwe 
	on qnle.embedding <=> dtwe.embedding <= 0.30
	and qnle.benchmark_name = dtwe.benchmark_name 
	and qnle.database_name = dtwe.database_name 
where qnle.benchmark_name = '__BENCHMARK_NAME__'
	and qnle.database_name = '__DATABASE_NAME__'
	and qnle.question_number = __QUESTION_NUMBER__
	and n = 1
)
select * from word_relation_matches
where ngram in (
	select ngram
	from word_relation_matches
	group by ngram
	having count(schema_identifier) > 2
)
;