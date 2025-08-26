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
from NlSqlBenchmark.QueryResult import QueryResult
from BenchmarkEmbedding.VectorSearchResults import VectorSearchResults, WordIdentifierDistance
from BenchmarkEmbedding.ValueReferenceProblemResults import ValueReferenceProblemItem, ValueReferenceProblemResults
from BenchmarkEmbedding.IdentifierAmbiguityProblemResults import IdentifierAmbiguityProblemItem, IdentifierAmbiguityProblemResults
from SubsetEvaluator.SchemaSubsetEvaluator import SchemaSubsetEvaluator
from SubsetEvaluator.QueryProfiler import QueryProfiler

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
            verbose: bool = False,
            build_database_on_init: bool = False,
            instantiate_embedding_model: bool = True,
            db_host_profile: str = "docker",
            db_host: str = "localhost",
            cuda_device: int = None
            ):
        if cuda_device != None:
            cuda_device = f":{cuda_device}"
        self.verbose = verbose
        self.benchmark_name = benchmark_name
        self.build_database_on_init = build_database_on_init
        self.current_working_directory = os.path.abspath(os.getcwd())
        if self.verbose:
            print("Current working directory:", self.current_working_directory)
        self.vector_length = vector_length

        self.kill_container_on_exit = kill_container_on_exit
        self.db_host_profile = db_host_profile
        self.db_host = db_host
        if self.db_host_profile == "docker":
            self.container = self._init_docker()
        self.db_conn = self._init_pgvector_db()

        if model_name != None:
            self.model_name = model_name
        else:
            self.model_name = "dunzhang/stella_en_1.5B_v5"
        if instantiate_embedding_model:
            self.embedding_model = SentenceTransformer(self.model_name, trust_remote_code=True)
            self.embedding_model.max_seq_length = 512
            self.embedding_model.tokenizer.padding_side="right"
            self.embedding_model.to(f"cuda{cuda_device}")
        else:
            self.embedding_model = None


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
        emb = self.embedding_model.encode(sequence)
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
            f"user=postgres host={self.db_host} port=5432 password=skalpel"
        )
        db_conn.autocommit = True
        try:
            db_conn.execute(f"CREATE DATABASE {self.benchmark_name}_benchmark_vector_db;")
        except psycopg.errors.DuplicateDatabase as e:
            pass
        db_conn.commit()
        db_conn.close()   

        db_conn = psycopg.connect(
            f"user=postgres dbname=benchmark_vector_db host={self.db_host} port=5432 password=skalpel dbname={self.benchmark_name}_benchmark_vector_db"
        )
        try:
            db_conn.execute(f"ALTER DATABASE {self.benchmark_name}_benchmark_vector_db SET work_mem = '{int(1000000/64)}MB'")
        except psycopg.errors.UniqueViolation as e:
            pass
        except psycopg.errors.InternalError_ as e:
            pass
        db_conn.commit()
        db_conn.close()

        db_conn = psycopg.connect(
            f"user=postgres dbname=benchmark_vector_db host={self.db_host} port=5432 password=skalpel dbname={self.benchmark_name}_benchmark_vector_db"
        )
        db_conn.autocommit = True
        db_conn.execute('CREATE EXTENSION IF NOT EXISTS vector')
        register_vector(db_conn)
        if self.build_database_on_init:
            db_creation_sql = open(
                f"./src/BenchmarkEmbedding/pgvector/queries/create_benchmark_vector_db.sql"
                ).read()
            db_creation_sql = db_creation_sql.replace("__VECTORLENGTH__", str(self.vector_length)) 
            cur = db_conn.cursor()
            try:
                cur.execute(db_creation_sql)
            except psycopg.errors.UniqueViolation as e:
                pass
        db_conn.commit()
        

        return db_conn
    


    def encode_benchmark_questions(self, benchmark: NlSqlBenchmark) -> None:

        add_question_to_benchmark_natural_language_questions_query = """
INSERT INTO benchmark_natural_language_questions(
    benchmark_name,
    database_name,
    question_number
) VALUES (
    %s,
    %s,
    %s
)
"""

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
            try:
                cursor.execute(
                    add_question_to_benchmark_natural_language_questions_query,
                    [
                        benchmark.name,
                        question.schema.database,
                        question.question_number
                    ]
                )
            except psycopg.errors.UniqueViolation as e:
                        pass
            self.db_conn.commit()
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
                                embedding
                            ]
                        )
                    except psycopg.errors.UniqueViolation as e:
                        pass
                    self.db_conn.commit()



    def encode_benchmark_gold_query_identifiers(self, benchmark: NlSqlBenchmark) -> None:

        add_to_benchmark_gold_queries_query = """
INSERT INTO benchmark_gold_queries(
    benchmark_name,
    naturalness,
    database_name,
    question_number
    ) VALUES (
    %s,
    %s,
    %s,
    %s
    )
"""

        insert_table_query = """
INSERT INTO benchmark_gold_query_tables(
    benchmark_name,
    naturalness,
    database_name,
    question_number,
    schema_identifier,
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

        insert_column_query = """
INSERT INTO benchmark_gold_query_columns(
    benchmark_name,
    naturalness,
    database_name,
    question_number,
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
    %s,
    %s,
    (%s)
)
"""
        cursor = self.db_conn.cursor()
        evaluator = SchemaSubsetEvaluator(use_result_cache=True)
        for question in tqdm(benchmark, total=len(benchmark), desc="Encoding benchmark Gold Query identifiers."):
            try:
                cursor.execute(
                    add_to_benchmark_gold_queries_query,
                    [
                        benchmark.name,
                        question.schema_naturalness,
                        question.schema.database,
                        question.question_number
                    ]
                )
            except psycopg.errors.UniqueViolation as e:
                        pass
            correct_subset = evaluator.get_correct_subset(question)
            for table in correct_subset.tables:
                table_embedding = self.get_embedding(table.name)
                try:
                    cursor.execute(
                        query=insert_table_query,
                        params=[
                            benchmark.name,
                            benchmark.naturalness,
                            question.schema.database,
                            question.question_number,
                            table.name,
                            self.model_name,
                            table_embedding
                        ]
                    )
                except psycopg.errors.UniqueViolation as e:
                        pass
                self.db_conn.commit()
                for column in table.columns:
                    column_embedding = self.get_embedding(column.name)
                    try:
                        cursor.execute(
                            query=insert_column_query,
                            params=[
                                benchmark.name,
                                benchmark.naturalness,
                                question.schema.database,
                                question.question_number,
                                table.name,
                                column.name,
                                self.model_name,
                                column_embedding
                            ]
                        )
                    except psycopg.errors.UniqueViolation as e:
                        pass
                    self.db_conn.commit()



    def encode_benchmark_gold_query_predicates(self, benchmark: NlSqlBenchmark) -> None:

        insert_predicate_query = """
INSERT INTO benchmark_gold_query_predicates(
    benchmark_name,
    naturalness,
    database_name,
    question_number,
    column_name,
    literal_value,
    embedding_model,
    column_embedding,
    literal_embedding
) VALUES (
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    (%s),
    (%s)
)
"""
        cursor = self.db_conn.cursor()
        profiler = QueryProfiler()
        for question in tqdm(benchmark, total=len(benchmark), desc="Encoding benchmark Gold Query predicates."):
            query_items = profiler.profile_query(query=question.query, dialect=question.query_dialect)
            a=1
            for key, value in query_items["stats"]:
                if "predicatecolumn" not in key:
                    continue
                column = key.split(" ")[1].replace("[", "").replace("]", "")
                column = column.replace("`", "")
                literal_value = " ".join(value.split(" ")[1:]).replace("'", "")
                if len(literal_value) == 0:
                    continue
                if literal_value[0] == "%":
                    literal_value = literal_value[1:]
                if literal_value[-1] == "%":
                    literal_value = literal_value[:-1]
                if literal_value.isnumeric():
                    continue
                column_embedding = self.get_embedding(column)
                literal_embedding = self.get_embedding(literal_value)
                try:
                    cursor.execute(
                        query=insert_predicate_query,
                        params=[
                            benchmark.name,
                            benchmark.naturalness,
                            question.schema.database,
                            question.question_number,
                            column,
                            literal_value,
                            self.model_name,
                            column_embedding,
                            literal_embedding
                        ]
                    )
                except psycopg.errors.UniqueViolation as e:
                    pass
                self.db_conn.commit()



    def encode_benchmark_values(self, benchmark: NlSqlBenchmark) -> None:

        text_types = [
            "CHAR", "VARCHAR", "TEXT", "NCHAR", "NVARCHAR", "NTEXT",  # SQL Server
            "CHARACTER", "CHARACTER VARYING", "CLOB", "TEXT"  # SQLite
        ]

        get_encoded_query = """
SELECT text_value
FROM text_value_embeddings
"""

        get_distinct_text_values_query = """
SELECT text_value from distinct_text_values
"""

        add_to_distinct_text_values_query = """
INSERT INTO distinct_text_values(
    text_value
) VALUES (
    %s
)
"""

        insert_value_embedding_query = """
INSERT INTO text_value_embeddings(
    text_value,
    embedding_model,
    embedding
) VALUES (
    %s,
    %s,
    (%s)
)
"""

        get_already_processed_tables = """
SELECT distinct table_name
FROM benchmark_text_values
WHERE 
    benchmark_name = '{}'
    AND database_name = '{}'
"""

        insert_benchmark_text_value_query = """
INSERT INTO benchmark_text_values(
    benchmark_name,
    database_name,
    table_name,
    column_name,
    text_value
) VALUES (
    %s,
    %s,
    %s,
    %s,
    %s
)
"""

        cursor = self.db_conn.cursor()
        result = cursor.execute(get_encoded_query)
        already_encoded = [row[0].upper() for row in result.fetchall()]
        result = cursor.execute(get_distinct_text_values_query)
        already_in_distinct_text_values = [row[0].upper() for row in result.fetchall()]

        for db in benchmark.databases:
            schema = benchmark.get_active_schema(database=db)
            result = cursor.execute(
                get_already_processed_tables.format(
                    benchmark.name,
                    db
                )
            )
            processed_tables = [row[0].upper() for row in result.fetchall()]
            for table in schema.tables:
                if table.name.upper() in processed_tables:
                    continue
                for column in table.columns:
                    # existing_values = [row[0].upper() for row in result.fetchall()]
                    # Ignore non-text columns:
                    if column.data_type.upper() not in text_types:
                        continue
                    # Ignore columns that end with ID (i.e., text-based ID values)
                    if column.name[-2:].upper() == "ID":
                        continue
                    query_result = benchmark.execute_query(
                        f"SELECT DISTINCT {column.name} FROM {table.name}",
                        database=db
                    )
                    if query_result.result_set == None:
                        continue
                    if column.name not in query_result.result_set.keys():
                        continue
                    col_values = query_result.result_set[column.name]
                    for v in tqdm(col_values, desc=f"Encoding values in {db}.{table.name}.{column.name}"):
                        if v == None or len(v) > 2700:
                            continue
                        if v not in already_in_distinct_text_values:
                            try:
                                cursor.execute(
                                    add_to_distinct_text_values_query,
                                    [v]
                                )
                                self.db_conn.commit()
                            except psycopg.errors.UniqueViolation as e:
                                self.db_conn.commit()
                            except psycopg.DataError as e:
                                self.db_conn.commit()
                                continue
                        try:
                            cursor.execute(
                                query=insert_benchmark_text_value_query,
                                params=[benchmark.name, db, table.name, column.name, v]
                            )
                        except psycopg.errors.UniqueViolation as e:
                            self.db_conn.commit()
                        except psycopg.DataError as e:
                            self.db_conn.commit()
                            continue
                        except psycopg.errors.ProgramLimitExceeded as e:
                            self.db_conn.commit()
                            continue
                        if v.upper() in already_encoded:
                            self.db_conn.commit()
                            continue
                        # print("\nDEBUG v", v)
                        v_emb = self.get_embedding(v, try_from_db=False)
                        # print("DEBUG type(v_emb)", type(v_emb))
                        try:
                            cursor.execute(
                                query=insert_value_embedding_query,
                                params=[v, self.model_name, v_emb]
                            )
                        except psycopg.errors.UniqueViolation as e:
                            self.db_conn.commit()
                        self.db_conn.commit()


    

    def encode_benchmark(self, benchmark: NlSqlBenchmark) -> (int, int):

        add_benchmark_to_benchmarks_query = """
INSERT INTO benchmarks(
    benchmark_name,
    database_name
) VALUES (
    %s,
    %s
)
"""

        add_table_to_database_table_names_query = """
INSERT INTO database_table_names(
    benchmark_name,
    database_name,
    table_name
) VALUES (
    %s,
    %s,
    %s
)
"""

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

        add_column_to_database_column_names_query = """
INSERT INTO database_column_names(
    benchmark_name,
    database_name,
    table_name,
    column_name
) VALUES (
    %s,
    %s,
    %s,
    %s
)
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
            cursor = self.db_conn.cursor()
            try:
                cursor.execute(
                    add_benchmark_to_benchmarks_query,
                    [
                        benchmark.name,
                        db
                    ]
                )
            except psycopg.errors.UniqueViolation as e:
                        self.db_conn.commit()
            schema = benchmark.get_active_schema(database=db)
            
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
                            add_table_to_database_table_names_query,
                            [
                                benchmark.name,
                                schema.database,
                                table.name
                            ]
                        )
                    except psycopg.errors.UniqueViolation as e:
                        self.db_conn.commit()
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
                    try:
                        cursor.execute(
                            add_column_to_database_column_names_query,
                            [
                                benchmark.name,
                                schema.database,
                                table.name,
                                column.name
                            ]
                        )
                    except psycopg.errors.UniqueViolation as e:
                        self.db_conn.commit()
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
    

    def _replace_strings_in_query_template(
            self, 
            query: str,
            database_name: str, 
            naturalness: str, 
            question_number: str
            ) -> str:
        replacements = {
            "__BENCHMARK_NAME__": self.benchmark_name,
            "__DATABASE_NAME__": database_name,
            "__EMBEDDING_MODEL__": self.model_name,
            "__NATURALNESS__": naturalness,
            "__QUESTION_NUMBER__": str(question_number)
        }
        for k in replacements:
            query = query.replace(k, replacements[k])
        return query


    def get_hidden_relations(
            self,
            database_name: str,
            question_number: int,
            naturalness: str = "Native"
            ) -> set:
        query_file = "./src/BenchmarkEmbedding/pgvector/queries/find_hidden_relations.sql"
        query = open(query_file, "r").read()
        query = self._replace_strings_in_query_template(
            query=query,
            database_name=database_name,
            question_number=question_number,
            naturalness=naturalness
            )
        cursor = self.db_conn.cursor()
        result = cursor.execute(query)
        self.db_conn.commit()
        return set([row[4] for row in result.fetchall()])



    def get_value_reference_problem_results(
                self,
                database_name: str,
                question_number: int,
                naturalness: str = "Native"
                ) -> ValueReferenceProblemResults:
        problem_results = ValueReferenceProblemResults(
            problem_columns=[]
        )
        query_file = f'src/BenchmarkEmbedding/pgvector/queries/find_value_reference_problem_predicates.sql'
        query = open(query_file, "r").read()
        query = self._replace_strings_in_query_template(
            query=query,
            database_name=database_name,
            question_number=question_number,
            naturalness=naturalness
            )
        cursor = self.db_conn.cursor()
        result = cursor.execute(query)
        self.db_conn.commit()
        for row in result.fetchall():
            #columns: table_name, column_name, text_value, ngram
            problem_item = ValueReferenceProblemItem(
                table_name=row[0],
                column_name=row[1],
                db_text_value=row[2],
                nlq_ngram=row[3],
            )
            problem_results.problem_columns.append(problem_item)                
        return problem_results
    



    def get_identifier_ambiguity_problem_results(
            self,
            database_name: str,
            question_number: int,
            naturalness: str= "Native"
            ) -> IdentifierAmbiguityProblemResults: 
        ambiguity_results = IdentifierAmbiguityProblemResults()
        relation_query_file = "src/BenchmarkEmbedding/pgvector/queries/find_ambiguous_relations.sql"
        attribute_query_file = "src/BenchmarkEmbedding/pgvector/queries/find_ambiguous_attributes.sql"
        query = open(relation_query_file, "r").read()
        query = self._replace_strings_in_query_template(
            query=query,
            database_name=database_name,
            question_number=question_number,
            naturalness=naturalness
        )
        cursor = self.db_conn.cursor()
        result = cursor.execute(query)
        self.db_conn.commit()
        for row in result.fetchall():
            #columns: ngram, schema_identifier, distance
            ambiguity_results.associate_table_with_word_nl(
                word_nl=row[0],
                table=SchemaTable(
                    name=row[1]
                )
            )
        query = open(attribute_query_file, "r").read()
        query = self._replace_strings_in_query_template(
            query=query,
            database_name=database_name,
            question_number=question_number,
            naturalness=naturalness
        )
        result = cursor.execute(query)
        self.db_conn.commit()
        for row in result.fetchall():
            #columns: ngram, table_name, schema_identifier, distance
            ambiguity_results.associate_column_with_word_nl(
                word_nl=row[0],
                column=TableColumn(
                    name=row[2], table_name=row[1]
                )
            )            
        return ambiguity_results










