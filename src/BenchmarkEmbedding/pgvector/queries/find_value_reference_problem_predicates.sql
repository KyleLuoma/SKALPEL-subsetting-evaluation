with word_column_matches as (
	select ngram, column_name 
	from benchmark_question_natural_language_question_word_embeddings qnl
	join benchmark_gold_query_predicates gqp on qnl.embedding <=> gqp.column_embedding <= 0.35
		and gqp.question_number = qnl.question_number 
		and gqp.database_name = qnl.database_name
	where qnl.database_name = '__DATABASE_NAME__'
	and gqp.naturalness = '__NATURALNESS__'
	and qnl.question_number = __QUESTION_NUMBER__
	),
question_identifiers as(
	select database_name, question_number, upper(table_name) as table_name, upper(schema_identifier) as column_name
	from benchmark_gold_query_columns gqc
	where gqc.database_name = '__DATABASE_NAME__'
		and gqc.naturalness = '__NATURALNESS__'
		and gqc.question_number = __QUESTION_NUMBER__
)
select qi.table_name, gqp.column_name, literal_value, ngram  
from benchmark_question_natural_language_question_word_embeddings qnl
join benchmark_gold_query_predicates gqp on qnl.embedding <=> gqp.literal_embedding  <= 0.35
	and gqp.question_number = qnl.question_number 
	and gqp.database_name = qnl.database_name
join question_identifiers qi on gqp.column_name = qi.column_name
	and gqp.question_number = qi.question_number
	and gqp.database_name = qi.database_name
where concat(ngram, gqp.column_name) not in (select concat(ngram, column_name) from word_column_matches)
	and qnl.database_name = '__DATABASE_NAME__'
	and gqp.naturalness = '__NATURALNESS__'
	and qnl.question_number = __QUESTION_NUMBER__