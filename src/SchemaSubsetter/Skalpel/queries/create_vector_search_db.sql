CREATE TABLE IF NOT EXISTS table_descriptions(
    database_name text,
    table_name text,
    description text,
    description_embedding vector(__VECTORLENGTH__)
);


CREATE TABLE IF NOT EXISTS table_description_sentences(
    database_name text,
    table_name text,
    description_sentence text,
    description_embedding vector(__VECTORLENGTH__)
);


CREATE TABLE IF NOT EXISTS column_names(
    database_name text,
    table_name text,
    column_name text,
    column_name_embedding vector(__VECTORLENGTH__)
);

CREATE TABLE IF NOT EXISTS column_name_naturalness(
    column_name text PRIMARY KEY,
    naturalness text
);

CREATE MATERIALIZED VIEW  IF NOT EXISTS column_names_with_naturalness AS
SELECT 
    cnm.database_name as database_name,
    cnm.table_name as table_name,
    cnm.column_name as column_name, 
    cnm.column_name_embedding as column_name_embedding, 
    cnat.naturalness as naturalness
FROM column_names cnm
LEFT JOIN column_name_naturalness cnat ON cnm.column_name = cnat.column_name;

CREATE INDEX ON column_names (column_name);
CREATE INDEX ON column_name_naturalness (column_name);
CREATE INDEX ON table_descriptions USING hnsw (description_embedding vector_cosine_ops);
CREATE INDEX ON table_description_sentences USING hnsw (description_embedding vector_cosine_ops);
CREATE INDEX ON column_names USING hnsw (column_name_embedding vector_cosine_ops);