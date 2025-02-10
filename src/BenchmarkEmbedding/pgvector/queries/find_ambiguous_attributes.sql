with word_attribute_matches as (
select ngram, table_name, schema_identifier, qnle.embedding <=> dcwe.embedding as distance
from benchmark_question_natural_language_question_word_embeddings qnle
join database_column_word_embeddings dcwe on dcwe.embedding <=> qnle.embedding <= 0.30
	and qnle.benchmark_name = dcwe.benchmark_name
	and qnle.database_name = dcwe.database_name
where qnle.benchmark_name = '__BENCHMARK_NAME__'
	and qnle.database_name = '__DATABASE_NAME__'
	and qnle.question_number = __QUESTION_NUMBER__
	and n = 1
)
select * from word_attribute_matches
where ngram in (
	select ngram
	from word_attribute_matches
	group by ngram
	having count(schema_identifier) > 2
)
;