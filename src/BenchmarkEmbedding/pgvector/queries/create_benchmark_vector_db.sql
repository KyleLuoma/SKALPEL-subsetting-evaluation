CREATE TABLE IF NOT EXISTS benchmarks(
    benchmark_name text,
    database_name text,
    PRIMARY KEY(benchmark_name, database_name)
)
;

CREATE TABLE IF NOT EXISTS database_table_names(
    benchmark_name text,
    database_name text,
    table_name text,
    PRIMARY KEY(benchmark_name, database_name, table_name),
    FOREIGN KEY (benchmark_name, database_name)
        REFERENCES benchmarks(benchmark_name, database_name)
)
;

CREATE TABLE IF NOT EXISTS database_column_names(
    benchmark_name text,
    database_name text,
    table_name text,
    column_name text,
    PRIMARY KEY(benchmark_name, database_name, table_name, column_name),
    FOREIGN KEY (benchmark_name, database_name, table_name)
        REFERENCES database_table_names(benchmark_name, database_name, table_name)
)
;

CREATE TABLE IF NOT EXISTS benchmark_natural_language_questions(
    benchmark_name text,
    database_name text,
    question_number integer,
    PRIMARY KEY(benchmark_name, database_name, question_number),
    FOREIGN KEY (benchmark_name, database_name) 
        REFERENCES benchmarks(benchmark_name, database_name)
)
;

CREATE TABLE IF NOT EXISTS benchmark_gold_queries(
    benchmark_name text,
    naturalness text,
    database_name text,
    question_number integer,
    PRIMARY KEY(benchmark_name, naturalness, database_name, question_number),
    FOREIGN KEY (benchmark_name, database_name, question_number) 
        REFERENCES benchmark_natural_language_questions(benchmark_name, database_name, question_number),
    FOREIGN KEY (benchmark_name, database_name) 
        REFERENCES benchmarks(benchmark_name, database_name)
)
;

CREATE TABLE IF NOT EXISTS database_table_word_embeddings(
    benchmark_name text,
    database_name text,
    schema_identifier text,
    embedding_model text,
    embedding vector(__VECTORLENGTH__),
    PRIMARY KEY(benchmark_name, database_name, embedding_model, schema_identifier),
    CONSTRAINT no_duplicate_table_tag UNIQUE (benchmark_name, database_name, embedding_model, schema_identifier),
    FOREIGN KEY (benchmark_name, database_name, schema_identifier) 
        REFERENCES database_table_names(benchmark_name, database_name, table_name)
)
;

CREATE TABLE IF NOT EXISTS database_column_word_embeddings(
    benchmark_name text,
    database_name text,
    table_name text,
    schema_identifier text,
    embedding_model text,
    embedding vector(__VECTORLENGTH__),
    PRIMARY KEY(benchmark_name, database_name, table_name, embedding_model, schema_identifier),
    CONSTRAINT no_duplicate_column_tag UNIQUE (benchmark_name, database_name, table_name, embedding_model, schema_identifier),
    FOREIGN KEY (benchmark_name, database_name, table_name, schema_identifier) 
        REFERENCES database_column_names(benchmark_name, database_name, table_name, column_name)
)
;

CREATE TABLE IF NOT EXISTS benchmark_question_natural_language_question_word_embeddings(
    benchmark_name text,
    database_name text,
    question_number integer,
    ngram text,
    n integer,
    embedding_model text,
    embedding vector(__VECTORLENGTH__),
    PRIMARY KEY(benchmark_name, database_name, question_number, ngram, embedding_model),
    FOREIGN KEY (benchmark_name, database_name, question_number) 
        REFERENCES benchmark_natural_language_questions(benchmark_name, database_name, question_number)
)
;

CREATE TABLE IF NOT EXISTS benchmark_gold_query_tables(
    benchmark_name text,
    naturalness text,
    database_name text,
    question_number integer,
    schema_identifier text,
    embedding_model text,
    embedding vector(__VECTORLENGTH__),
    CONSTRAINT no_duplicate_query_table_identifiers UNIQUE (
        benchmark_name, naturalness, database_name, question_number, schema_identifier, embedding_model
        ),
    PRIMARY KEY(benchmark_name, naturalness, database_name, question_number, schema_identifier, embedding_model),
    FOREIGN KEY (benchmark_name, database_name, schema_identifier, embedding_model) 
        REFERENCES database_table_word_embeddings(benchmark_name, database_name, schema_identifier, embedding_model),
    FOREIGN KEY (benchmark_name, naturalness, database_name, question_number) 
        REFERENCES benchmark_gold_queries(benchmark_name, naturalness, database_name, question_number)
)
;

CREATE TABLE IF NOT EXISTS benchmark_gold_query_columns(
    benchmark_name text,
    naturalness text,
    database_name text,
    question_number integer,
    table_name text,
    schema_identifier text,
    embedding_model text,
    embedding vector(__VECTORLENGTH__),
    CONSTRAINT no_duplicate_query_column_identifiers UNIQUE (
        benchmark_name, naturalness, database_name, question_number, table_name, schema_identifier, embedding_model
        ),
    PRIMARY KEY(benchmark_name, naturalness, database_name, question_number, table_name, schema_identifier, embedding_model),
    FOREIGN KEY (benchmark_name, naturalness, database_name, question_number, table_name, embedding_model) 
        REFERENCES benchmark_gold_query_tables(benchmark_name, naturalness, database_name, question_number, schema_identifier, embedding_model),
    FOREIGN KEY (benchmark_name, database_name, table_name, schema_identifier, embedding_model) 
        REFERENCES database_column_word_embeddings(benchmark_name, database_name, table_name, schema_identifier, embedding_model)
)
;

CREATE TABLE IF NOT EXISTS benchmark_gold_query_predicates(
    benchmark_name text,
    naturalness text,
    database_name text,
    question_number integer,
    column_name text,
    literal_value text,
    embedding_model text,
    column_embedding vector(__VECTORLENGTH__),
    literal_embedding vector(__VECTORLENGTH__),
    CONSTRAINT no_duplicate_query_predicates UNIQUE (
        benchmark_name, naturalness, database_name, question_number, column_name, literal_value, embedding_model
    ),
    PRIMARY KEY(benchmark_name, naturalness, database_name, question_number, column_name, literal_value, embedding_model),
    FOREIGN KEY (benchmark_name, naturalness, database_name, question_number) 
        REFERENCES benchmark_gold_queries(benchmark_name, naturalness, database_name, question_number)
)
;

CREATE TABLE IF NOT EXISTS distinct_text_values(
    text_value text,
    CONSTRAINT no_duplicate_distinct_text_values UNIQUE (text_value),
    PRIMARY KEY(text_value)
)
;

CREATE TABLE IF NOT EXISTS text_value_embeddings(
    text_value text,
    embedding_model text,
    embedding vector(__VECTORLENGTH__),
    CONSTRAINT no_duplicate_text_value_embeddings UNIQUE (
        text_value, embedding_model
    ),
    PRIMARY KEY(text_value, embedding_model),
    FOREIGN KEY(text_value) REFERENCES distinct_text_values(text_value)
)
;

CREATE TABLE IF NOT EXISTS benchmark_text_values(
    benchmark_name text,
    database_name text,
    table_name text,
    column_name text,
    text_value text,
    CONSTRAINT no_duplicate_benchmark_text_values UNIQUE (
        benchmark_name, database_name, table_name, column_name, text_value 
    ),
    PRIMARY KEY(benchmark_name, database_name, table_name, column_name, text_value),
    FOREIGN KEY (text_value) REFERENCES distinct_text_values(text_value),
    FOREIGN KEY (benchmark_name, database_name) REFERENCES benchmarks(benchmark_name, database_name)
)
;





CREATE INDEX ON text_value_embeddings (text_value);
CREATE INDEX ON benchmark_text_values (text_value);
CREATE INDEX ON benchmark_gold_query_predicates USING hnsw (column_embedding vector_cosine_ops);
CREATE INDEX ON benchmark_gold_query_predicates USING hnsw (literal_embedding vector_cosine_ops);
CREATE INDEX ON text_value_embeddings USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ON database_column_word_embeddings USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ON database_table_word_embeddings USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ON benchmark_question_natural_language_question_word_embeddings USING hnsw (embedding vector_cosine_ops);
