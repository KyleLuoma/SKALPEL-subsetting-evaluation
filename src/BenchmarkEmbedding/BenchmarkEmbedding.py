import torch
import numpy
import time
from sentence_transformers import SentenceTransformer

from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from BenchmarkEmbedding.VectorSearchResults import VectorSearchResults, WordIdentifierDistance

import docker
import docker.errors
import docker.models
import docker.models.containers

import psycopg
import os
from tqdm import tqdm
from pgvector.psycopg import register_vector


class BenchmarkEmbedding:

    def __init__(
            self, 
            benchmark_name: str,
            model_name: str = None,
            vector_length: int = 1024,
            kill_container_on_exit: bool = False,
            verbose: bool = False
            ):
        self.verbose = verbose
        self.current_working_directory = os.path.abspath(os.getcwd())
        if self.verbose:
            print("Current working directory:", self.current_working_directory)
        self.vector_length = vector_length

        self.kill_container_on_exit = kill_container_on_exit
        self.container = self._init_docker()
        self.db_conn = self._init_pgvector_db()

        if model_name != None:
            self.model_name = model_name
        else:
            self.model_name = "dunzhang/stella_en_1.5B_v5"
        self.benchmark_name = benchmark_name
        self.embedding_model = SentenceTransformer(self.model_name, trust_remote_code=True)
        self.embedding_model.max_seq_length = 512
        self.embedding_model.tokenizer.padding_side="right"
        self.embedding_model.to("cuda")


    def __del__(self):
        if self.kill_container_on_exit and self.container != None:
            print("BenchmarkEmbedding is stopping Docker container 'skalpel-pgvector'")
            try:
                self.container.kill()
            except docker.errors.APIError as e:
                pass


    def get_string_similarities(self, string1: str, string2: str) -> float:
        emb1 = self.get_embedding(string1)
        emb2 = self.get_embedding(string2)
        distance = self.get_similarity(emb1, emb2)
        return float(distance[0][0])
        

    def get_embedding(self, sequence: str, try_from_db: bool = True) -> numpy.ndarray:
        if try_from_db:
            cursor = self.db_conn.cursor()
            tab_result = cursor.execute(
                "SELECT embedding FROM database_table_word_embeddings WHERE schema_identifier = %s and embedding_model = %s",
                [sequence, self.model_name]
                )
            results = tab_result.fetchall()
            if len(results) > 0:
                vector = results[0][0]
                return vector
            col_result = cursor.execute(
                "SELECT embedding FROM database_column_word_embeddings WHERE schema_identifier = %s and embedding_model = %s",
                [sequence, self.model_name]
            )
            results = col_result.fetchall()
            if len(results) > 0:
                vector = results[0][0]
                return vector
        # emb = self.embedding_model.encode([sequence], prompt_name="s2s_query")
        emb = self.embedding_model.encode([sequence])
        return emb
    

    def get_similarity(self, embedding1: numpy.ndarray, embedding2: numpy.ndarray) -> torch.tensor:
        return self.embedding_model.similarity(embedding1, embedding2)


    def _init_docker(self, retry_attempts: int = 5) -> docker.models.containers.Container:       

        database_running = True
        client = docker.from_env()
        try:
            db_conn = psycopg.connect(
                "user=postgres host=localhost port=5432 password=skalpel"
            )
            container = client.containers.get("skalpel-pgvector")
            container.start()
        except:
            database_running = False

        retry_counts = 0

        while not database_running and retry_counts < retry_attempts:    
            retry_counts += 1
            try:
                client.images.get('skalpel-pgvector')
            except docker.errors.ImageNotFound as e:
                if self.verbose:
                    print("The 'skalpel-pgvector' Docker image is not available locally. Run the install script /src/BenchmarkEmbedding/pgvector/install_pgvector.sh before instantiating the BenchmarkEmbedding class.")
                raise e
            
            try:
                if self.verbose:
                    print("Loading Docker container 'skalpel-pgvector'")
                container = client.containers.get("skalpel-pgvector")
                container.start()
            except docker.errors.NotFound:
                if self.verbose:
                    print("Container not found, running container for the first time on port 5432.")
                container = client.containers.run(
                    "skalpel-pgvector", 
                    ports={5432:5432}, 
                    name="skalpel-pgvector",
                    environment={"POSTGRES_PASSWORD": "skalpel"},
                    volumes={f"pgdata": {"bind": "/var/lib/postgresql/data", "mode": "rw"}},
                    detach=True
                    )
                

            database_running = True
            try:
                db_conn = psycopg.connect(
                    "user=postgres host=localhost port=5432 password=skalpel"
                )
            except psycopg.OperationalError as e:
                if self.verbose:
                    print(e)
                time.sleep(1)
                database_running = False
            
        return container
    

    def _init_pgvector_db(self) -> psycopg.connection.Connection:
        db_conn = psycopg.connect(
            "user=postgres host=localhost port=5432 password=skalpel"
        )
        db_conn.autocommit = True
        try:
            db_conn.execute("CREATE DATABASE benchmark_vector_db;")
        except psycopg.errors.DuplicateDatabase as e:
            pass
        db_conn.execute('CREATE EXTENSION IF NOT EXISTS vector')
        db_conn.commit()
        db_conn.close()

        db_creation_sql = open(f".\\src\\BenchmarkEmbedding\\pgvector\\queries\\create_benchmark_vector_db.sql").read()
        db_creation_sql = db_creation_sql.replace("__VECTORLENGTH__", str(self.vector_length))       
        db_conn = psycopg.connect(
            "user=postgres dbname=benchmark_vector_db host=localhost port=5432 password=skalpel"
        )
        db_conn.execute('CREATE EXTENSION IF NOT EXISTS vector')
        register_vector(db_conn)
        cur = db_conn.cursor()
        cur.execute(db_creation_sql)
        db_conn.commit()
        return db_conn
    


    def encode_benchmark_questions(self, benchmark: NlSqlBenchmark) -> None:

        insert_word_query = """
INSERT INTO benchmark_question_natural_language_question_word_embeddings(
    benchmark_name,
    database_name,
    question_number,
    ngram,
    n,
    embedding_model,
    embedding
) VALUES (
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    (%s)
)
"""
        cursor = self.db_conn.cursor()
        for question in tqdm(benchmark, total=len(benchmark), desc="Encoding benchmark NL questions"):
            ngrams = {1: [], 2: [], 3: []}
            words = question.question.split()
            for n in [1, 2, 3]:
                for i in range(0, len(words) - (n - 1)):
                    ngrams[n].append(" ".join(words[i : i + n]))
            for n in ngrams:
                for ngram in ngrams[n]:
                    embedding = self.get_embedding(ngram, try_from_db=False)
                    try:
                        cursor.execute(
                            insert_word_query,
                            [
                                self.benchmark_name, 
                                question.schema.database, 
                                question.question_number,
                                ngram,
                                n,
                                self.model_name,
                                embedding[0]
                            ]
                        )
                    except psycopg.errors.UniqueViolation as e:
                        pass
                    self.db_conn.commit()


    

    def encode_benchmark(self, benchmark: NlSqlBenchmark) -> (int, int):

        insert_table_query = """
INSERT INTO database_table_word_embeddings(
    benchmark_name,
    database_name,
    schema_identifier,
    embedding_model,
    embedding
) VALUES (
    %s,
    %s,
    %s,
    %s,
    (%s)
)
"""

        get_encoded_tables_query = """
SELECT schema_identifier 
FROM database_table_word_embeddings 
WHERE
    benchmark_name = %s
    AND database_name = %s
    AND embedding_model = %s
"""

        insert_column_query = """
INSERT INTO database_column_word_embeddings(
    benchmark_name,
    database_name,
    table_name,
    schema_identifier,
    embedding_model,
    embedding
) VALUES (
    %s,
    %s,
    %s,
    %s,
    %s,
    (%s)
)
"""

        get_encoded_columns_query = """
        SELECT schema_identifier 
        FROM database_column_word_embeddings 
        WHERE
            benchmark_name = %s
            AND database_name = %s
            AND table_name = %s
            AND embedding_model = %s
"""

        tables_inserted = 0
        columns_inserted = 0
        for db in benchmark.databases:
            schema = benchmark.get_active_schema(database=db)
            cursor = self.db_conn.cursor()
            result = cursor.execute(
                get_encoded_tables_query,
                [benchmark.name, schema.database, self.model_name]
                )
            encoded_tables = [row[0] for row in result.fetchall()]

            for table in tqdm(schema.tables, desc=f"Encoding tables and columns in {schema.database}."):
                if table.name not in encoded_tables:
                    table_emb = self.embedding_model.encode(table.name)
                    try:
                        cursor.execute(
                            insert_table_query,
                            [
                                benchmark.name,
                                schema.database,
                                table.name,
                                self.model_name,
                                table_emb
                            ]
                        )
                        self.db_conn.commit()
                        tables_inserted += 1
                    except psycopg.errors.UniqueViolation as e:
                        self.db_conn.commit()
                result = cursor.execute(
                    get_encoded_columns_query,
                    [benchmark.name, schema.database, table.name, self.model_name]
                )

                encoded_columns = [row[0] for row in result.fetchall()]
                for column in table.columns:
                    if column.name in encoded_columns:
                        result = cursor.execute(
                            "SELECT embedding FROM database_column_word_embeddings WHERE schema_identifier = %s AND embedding_model = %s LIMIT 1",
                            [column.name, self.model_name]
                        )
                        column_emb = result.fetchone()[0]
                    else:    
                        column_emb = self.embedding_model.encode(column.name)
                    try:
                        cursor.execute(
                            insert_column_query,
                            [
                                benchmark.name,
                                schema.database,
                                table.name,
                                column.name,
                                self.model_name,
                                column_emb
                            ]
                        )
                        self.db_conn.commit()
                        columns_inserted += 1
                    except psycopg.errors.UniqueViolation as e:
                        self.db_conn.commit()

        return (tables_inserted, columns_inserted)
    

    def get_identifiers_from_semantic_search(
            self,
            search_word: str,
            database_name: str,
            distance_threshold: float = 0.5
            ) -> VectorSearchResults:
        
        word_embedding = self.embedding_model.encode(search_word)
        search_results = VectorSearchResults(
            search_word=search_word,
            database_name=database_name,
            distance_threshold=distance_threshold
        )
        
        table_retrieval_query = """
SELECT schema_identifier, embedding <=> (%s) as distance
FROM database_table_word_embeddings
WHERE
    benchmark_name = %s
    AND database_name = %s
    AND embedding <=> (%s) <= %s
"""
        column_retrieval_query = """
SELECT schema_identifier, table_name, embedding <=> (%s) as distance
FROM database_column_word_embeddings
WHERE
    benchmark_name = %s
    AND database_name = %s
    AND embedding <=> (%s) <= %s
"""

        cursor = self.db_conn.cursor()
        results = cursor.execute(
            table_retrieval_query,
            [word_embedding, self.benchmark_name, database_name, word_embedding, distance_threshold]
            # [word_embedding, word_embedding, distance_threshold]
        )
        search_results.tables = [
            WordIdentifierDistance(
                search_word= search_word, database_identifier=row[0], distance=row[1]
                ) for row in results.fetchall()
            ]

        results = cursor.execute(
            column_retrieval_query,
            [word_embedding, self.benchmark_name, database_name, word_embedding, distance_threshold]
            # [word_embedding, word_embedding, distance_threshold]
        )
        search_results.columns = [
            WordIdentifierDistance(
                search_word=search_word, database_identifier=f"{row[1]}.{row[0]}", distance=row[2]
                ) for row in results.fetchall()
                ]
        return search_results



