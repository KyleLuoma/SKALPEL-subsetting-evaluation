import torch
import numpy
import time
from sentence_transformers import SentenceTransformer

from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark

import docker
import docker.errors
import docker.models
import docker.models.containers

import psycopg
import os
from pgvector.psycopg import register_vector


class BenchmarkEmbedding:

    def __init__(
            self, 
            benchmark_name: str,
            model_name: str = None,
            vector_length: int = 1024,
            kill_container_on_exit: bool = True,
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

        


    def get_embedding(self, sequence: str) -> numpy.ndarray:
        return self.embedding_model.encode([sequence], prompt_name="s2s_query")
    

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
    

    def _init_pgvector_db(self) -> psycopg.connection:
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

        db_creation_sql = open(f"{self.current_working_directory}\\queries\\create_benchmark_vector_db.sql").read()
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
