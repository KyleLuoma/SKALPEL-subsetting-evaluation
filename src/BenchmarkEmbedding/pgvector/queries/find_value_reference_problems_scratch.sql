select *
from text_value_embeddings tve
join benchmark_text_values btv on tve.text_value =btv.text_value
where btv.database_name = 'NTSB'
and tve.embedding <=> (
	select embedding from text_value_embeddings where text_value like '%TOYOTA%' limit 1
	) <= 0.35
;

select distinct table_name
from benchmark_text_values btv 
where btv.database_name = 'NTSB'
;

select *
from benchmark_text_values btv 
where btv.database_name = 'NTSB'
and btv.column_name = 'Make'
and text_value like '%TOYOTA%'
;

-- Table matching
with values as (
select ngram, text_value, qnl.embedding <=> tve.embedding as distance
from benchmark_question_natural_language_question_word_embeddings qnl
join text_value_embeddings tve on qnl.embedding <=> tve.embedding <= 0.2
where qnl.database_name = 'ASIS_20161108_HerpInv_Database'
and qnl.question_number = 0
),
word_value_matches as (
select table_name, column_name, btv.text_value, ngram, distance 
from benchmark_text_values btv 
join values on values.text_value = btv.text_value
where btv.database_name = 'ASIS_20161108_HerpInv_Database'
),
gold_query_matches as (
select qnl.question_number, gqt.question_number, qnl.ngram, gqt.schema_identifier 
from benchmark_gold_query_tables gqt
join benchmark_question_natural_language_question_word_embeddings qnl on gqt.embedding <=> qnl.embedding <= 0.35
	and qnl.database_name = gqt.database_name
	and qnl.question_number = gqt.question_number
where qnl.database_name = 'ASIS_20161108_HerpInv_Database'
and gqt.question_number = 0
)
select table_name, column_name, text_value, ngram, distance, 'FALSE' as match
from word_value_matches
where word_value_matches.table_name not in (
	select schema_identifier from gold_query_matches
	)
	and word_value_matches.table_name in (
		select schema_identifier from benchmark_gold_query_tables gqt
		where gqt.question_number = 0
	)
union
select table_name, column_name, text_value, ngram, distance, 'TRUE' as match
from word_value_matches
where word_value_matches.table_name in (
	select schema_identifier from gold_query_matches
	)
	and word_value_matches.table_name in (
		select schema_identifier from benchmark_gold_query_tables gqt
		where gqt.question_number = 0
	)
;

-- Column matching
with values as (
select ngram, text_value, qnl.embedding <=> tve.embedding as distance
from benchmark_question_natural_language_question_word_embeddings qnl
join text_value_embeddings tve on qnl.embedding <=> tve.embedding <= 0.2
where qnl.database_name = 'NTSB'
and qnl.question_number = 33
),
word_value_matches as (
select table_name, column_name, btv.text_value, ngram, distance 
from benchmark_text_values btv 
join values on values.text_value = btv.text_value
where btv.database_name = 'NTSB'
),
gold_query_matches as (
select qnl.question_number, qnl.ngram, gqc.schema_identifier
from benchmark_gold_query_columns gqc
join benchmark_question_natural_language_question_word_embeddings qnl on gqc.embedding <=> qnl.embedding <= 0.35
	and qnl.database_name = gqc.database_name
	and qnl.question_number = gqc.question_number
where qnl.database_name = 'NTSB'
and gqc.question_number = 33
)
select table_name, column_name, text_value, ngram, distance, 'FALSE' as match
from word_value_matches
where word_value_matches.table_name not in (
	select schema_identifier from gold_query_matches
	)
union
select table_name, column_name, text_value, ngram, distance, 'TRUE' as match
from word_value_matches
where word_value_matches.table_name in (
	select schema_identifier from gold_query_matches
	)
;