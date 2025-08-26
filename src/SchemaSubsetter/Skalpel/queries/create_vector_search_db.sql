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


CREATE INDEX ON table_descriptions USING hnsw (description_embedding vector_cosine_ops);
CREATE INDEX ON table_description_sentences USING hnsw (description_embedding vector_cosine_ops);