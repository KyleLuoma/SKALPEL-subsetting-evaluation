CREATE TABLE IF NOT EXISTS database_table_word_embeddings(
    benchmark_name text,
    database_name text,
    schema_identifier text,
    embedding_model text,
    embedding vector(__VECTORLENGTH__),
    PRIMARY KEY(benchmark_name, database_name, embedding_model, schema_identifier),
    CONSTRAINT no_duplicate_table_tag UNIQUE (benchmark_name, database_name, embedding_model, schema_identifier)
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
    CONSTRAINT no_duplicate_column_tag UNIQUE (benchmark_name, database_name, table_name, embedding_model, schema_identifier)
)
;

CREATE TABLE IF NOT EXISTS database_text_value_embeddings(
    benchmark_name text,
    database_name text,
    value text,
    embedding_model text,
    embedding vector(__VECTORLENGTH__),
    PRIMARY KEY(benchmark_name, database_name, value, embedding_model),
    CONSTRAINT no_duplicate_text_value_embeddings UNIQUE (benchmark_name, database_name, value, embedding_model)
)
;

CREATE TABLE IF NOT EXISTS database_text_value_columns(
    benchmark_name text,
    database_name text,
    value text,
    table_name text,
    column_name text,
    CONSTRAINT no_duplicate_text_value_columns UNIQUE (benchmark_name, database_name, value, table_name, column_name)
)
;

CREATE TABLE IF NOT EXISTS benchmark_question_natural_language_question_word_embeddings(
    benchmark_name text,
    database_name text,
    question_number integer,
    ngram text,
    n integer,
    embedding_model text,
    embedding vector(__VECTORLENGTH__)
)
;

CREATE INDEX ON database_column_word_embeddings USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ON database_table_word_embeddings USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ON database_text_value_embeddings USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ON benchmark_question_natural_language_question_word_embeddings USING hnsw (embedding vector_cosine_ops);