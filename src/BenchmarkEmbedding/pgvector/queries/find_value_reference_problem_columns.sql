-- Query for exposing columns not matched due to the value reference problem, as defined in the SKALPEL paper
with values as (
select ngram, text_value, qnl.embedding <=> tve.embedding as distance
from benchmark_question_natural_language_question_word_embeddings qnl
join text_value_embeddings tve on qnl.embedding <=> tve.embedding <= 0.2
where qnl.database_name = '__DATABASE_NAME__'
and qnl.benchmark_name = '__BENCHMARK_NAME__'
and qnl.question_number = __QUESTION_NUMBER__
),
word_value_matches as (
select table_name, column_name, btv.text_value, ngram, distance 
from benchmark_text_values btv 
join values on values.text_value = btv.text_value
where btv.database_name = '__DATABASE_NAME__'
and btv.benchmark_name = '__BENCHMARK_NAME__'
),
gold_query_matches as (
select qnl.question_number, gqc.question_number, qnl.ngram, gqc.schema_identifier 
from benchmark_gold_query_columns gqc
join benchmark_question_natural_language_question_word_embeddings qnl on gqc.embedding <=> qnl.embedding <= 0.35
	and qnl.database_name = gqc.database_name
	and qnl.question_number = gqc.question_number
where qnl.database_name = '__DATABASE_NAME__'
and qnl.benchmark_name = '__BENCHMARK_NAME__'
and gqc.question_number = __QUESTION_NUMBER__
and gqc.naturalness = '__NATURALNESS__'
),
gold_query_tables as (
	select schema_identifier 
	from benchmark_gold_query_tables gqt
	where gqt.question_number = __QUESTION_NUMBER__
	and gqt.database_name = '__DATABASE_NAME__'
),
gold_query_attributes as (
	select schema_identifier
	from benchmark_gold_query_columns gqc
	where gqc.question_number = __QUESTION_NUMBER__
	and gqc.database_name = '__DATABASE_NAME__'
)
select table_name, column_name, text_value, ngram, distance, 'FALSE' as match
from word_value_matches
where word_value_matches.table_name not in (
	select schema_identifier from gold_query_matches
	)
	and word_value_matches.table_name in (select schema_identifier from gold_query_tables)
	and word_value_matches.column_name in (select schema_identifier from gold_query_attributes)
union
select table_name, column_name, text_value, ngram, distance, 'TRUE' as match
from word_value_matches
where word_value_matches.table_name in (
	select schema_identifier from gold_query_matches
	)
	and word_value_matches.table_name in (select schema_identifier from gold_query_tables)
	and word_value_matches.column_name in (select schema_identifier from gold_query_attributes)
;