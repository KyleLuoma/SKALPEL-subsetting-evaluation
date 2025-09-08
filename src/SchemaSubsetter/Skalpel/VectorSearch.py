import numpy
from SchemaSubsetter.Skalpel.SkalpelVectorSearchResults import VectorSearchResults, WordIdentifierDistance
from sentence_transformers import SentenceTransformer
import psycopg
from pgvector.psycopg import register_vector


class VectorSearch:

    def __init__(
            self,
            benchmark_name: str,
            model_name: str = None,
            vector_length: int = 1024,
            db_host: str = "localhost"            
            ):
        self.db_host = db_host
        self.benchmark_name = benchmark_name
        self.vector_length = vector_length
        self.db_conn = self._init_pgvector_db()
        if model_name != None:
            self.model_name = model_name
        else:
            self.model_name = "dunzhang/stella_en_1.5B_v5"
        self.embedding_model = SentenceTransformer(self.model_name, trust_remote_code=True)
        self.embedding_model.max_seq_length = 8000
        self.embedding_model.tokenizer.padding_side="right"
        self.embedding_model.to("cuda")



    def __del__(self):
        if hasattr(self, "embedding_model") and self.embedding_model is not None:
            try:
                self.embedding_model.to("cpu")
                del self.embedding_model
            except Exception:
                pass


    def _init_pgvector_db(self) -> psycopg.connection.Connection:
        db_conn = psycopg.connect(
            f"user=postgres host={self.db_host} port=5432 password=skalpel"
        )
        db_conn.autocommit = True
        try:
            db_conn.execute(f"CREATE DATABASE {self.benchmark_name}_skalpel_subsetter_vector_db;")
        except psycopg.errors.DuplicateDatabase as e:
            pass
        db_conn.commit()
        db_conn.close()   

        db_conn = psycopg.connect(
            f"user=postgres host={self.db_host} port=5432 password=skalpel dbname={self.benchmark_name}_skalpel_subsetter_vector_db"
        )
        try:
            db_conn.execute(f"ALTER DATABASE {self.benchmark_name}_skalpel_subsetter_vector_db SET work_mem = '{int(1000000/64)}MB'")
        except psycopg.errors.UniqueViolation as e:
            pass
        except psycopg.errors.InternalError_ as e:
            pass
        db_conn.commit()
        db_conn.close()

        db_conn = psycopg.connect(
            f"user=postgres host={self.db_host} port=5432 password=skalpel dbname={self.benchmark_name}_skalpel_subsetter_vector_db"
        )
        db_conn.autocommit = True
        db_conn.execute('CREATE EXTENSION IF NOT EXISTS vector')
        register_vector(db_conn)
        db_creation_sql = open(
            f"src/SchemaSubsetter/Skalpel/queries/create_vector_search_db.sql"
            ).read()
        db_creation_sql = db_creation_sql.replace("__VECTORLENGTH__", str(self.vector_length)) 
        cur = db_conn.cursor()
        try:
            cur.execute(db_creation_sql)
        except psycopg.errors.UniqueViolation as e:
            pass
        db_conn.commit()
        return db_conn


    def recreate_db(self):
        
        self.db_conn.close()
        self.db_conn = psycopg.connect(
            f"user=postgres host={self.db_host} port=5432 password=skalpel"
        )
        cursor = self.db_conn.cursor()
        self.db_conn.autocommit = True
        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {self.benchmark_name}_skalpel_subsetter_vector_db;")
        except psycopg.errors.InvalidCatalogName:
            pass
        self.db_conn.commit()
        self.db_conn.close()
        self.db_conn = self._init_pgvector_db()



    def encode_string(self, sequence: str) -> numpy.ndarray:
        emb = self.embedding_model.encode(sequence)
        return emb
    


    def encode_list_of_strings(self, string_list: list[str]) -> list[numpy.ndarray]:
        emb_list = self.embedding_model.encode(string_list)
        return emb_list.tolist()
    
    

    def encode_table_description_into_db(self, db_name: str, table_name: str, table_description: str):
        emb = self.encode_string(table_description)
        query = "INSERT INTO table_descriptions(database_name, table_name, description, description_embedding) VALUES (%s, %s, %s, (%s))"
        cursor = self.db_conn.cursor()
        cursor.execute(
            query=query,
            params=[
                db_name,
                table_name,
                table_description,
                emb
            ]
        )
        self.db_conn.commit()



    def get_similar_table_descriptions_from_db(
            self, 
            db_name: str, 
            input_sequence: str, 
            distance_threshold: float = 0.5,
            schema_proportion: float = 0.5
            ) -> VectorSearchResults:
        description_embedding = self.encode_string(input_sequence)
        search_query = """
SELECT table_name, description, description_embedding <=> (%s) as distance
FROM table_descriptions 
WHERE 
    database_name = %s 
    AND description_embedding <=> (%s) <= %s
ORDER BY distance ASC
LIMIT %s * (SELECT COUNT(*) FROM table_descriptions WHERE database_name = %s)
"""
        cursor = self.db_conn.cursor()
        query_result = cursor.execute(
            search_query,
            [
                description_embedding,
                db_name,
                description_embedding,
                distance_threshold,
                schema_proportion,
                db_name
            ]
        )
        search_results = VectorSearchResults(
            search_word=input_sequence,
            database_name=db_name,
            distance_threshold=distance_threshold
        )
        search_results.tables = [
            WordIdentifierDistance(
                search_word=input_sequence, database_identifier=row[0], distance=row[2]
            ) for row in query_result.fetchall()
        ]
        return search_results


    def get_table_description_from_db(
            self, db_name: str, table_name: str
            ) -> str:
        retrieve_query = """
SELECT description FROM table_descriptions
WHERE
    database_name = %s
    AND table_name = %s
"""
        cursor = self.db_conn.cursor()
        query_result = cursor.execute(
            retrieve_query,
            [db_name, table_name]
        )
        return query_result.fetchone()[0]



    def encode_table_description_sentences_into_db(self, db_name: str, table_name: str, table_description: str):
        emb = self.encode_string(table_description)
        query = "INSERT INTO table_description_sentences(database_name, table_name, description_sentence, description_embedding) VALUES (%s, %s, %s, (%s))"
        cursor = self.db_conn.cursor()
        for sentence in table_description.split("."):
            cursor.execute(
                query=query,
                params=[
                    db_name,
                    table_name,
                    sentence,
                    emb
                ]
            )
        self.db_conn.commit()



    def get_similar_table_description_sentences_from_db(
        self, 
        db_name: str, 
        input_sequence: str, 
        distance_threshold: float = 0.5,
        schema_proportion: float = 0.5
        ) -> VectorSearchResults:
        description_embedding = self.encode_string(input_sequence)
        search_query = """
    SELECT table_name, description_sentence, description_embedding <=> (%s) as distance
    FROM table_description_sentences 
    WHERE 
        database_name = %s 
        AND description_embedding <=> (%s) <= %s
    ORDER BY distance ASC
    LIMIT %s * (SELECT COUNT(*) FROM table_description_sentences WHERE database_name = %s)
    """
        cursor = self.db_conn.cursor()
        query_result = cursor.execute(
        search_query,
        [
            description_embedding,
            db_name,
            description_embedding,
            distance_threshold,
            schema_proportion,
            db_name
        ]
        )
        search_results = VectorSearchResults(
        search_word=input_sequence,
        database_name=db_name,
        distance_threshold=distance_threshold
        )
        search_results.tables = [
        WordIdentifierDistance(
            search_word=input_sequence, database_identifier=row[0], distance=row[2]
        ) for row in query_result.fetchall()
        ]
        return search_results
    


    def query_vector_db(self, query: str, params: list) -> list[tuple]:
        cursor = self.db_conn.cursor()
        query_result = cursor.execute(
            query,
            params
        )
        results = [t for t in query_result.fetchall()]
        return results