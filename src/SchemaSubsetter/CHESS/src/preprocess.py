import os
from pathlib import Path
import logging
import pickle
from dotenv import load_dotenv
from langchain_chroma import Chroma
from langchain.schema.document import Document
from langchain_openai import OpenAIEmbeddings

from SchemaSubsetter.CHESS.src.database_utils.db_values.preprocess import make_lsh
from tqdm import tqdm


# from database_utils.db_catalog.csv_utils import load_tables_description

##### SKALPEL Imports #####
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark


load_dotenv(dotenv_path="./src/SchemaSubsetter/CHESS/.env", override=True)
EMBEDDING_FUNCTION = OpenAIEmbeddings(model="text-embedding-3-large")

# SKALPEL MOD: Added benchmark argument to replace process of loading table descriptions from the HD
def make_db_context_vec_db(db_id: str, db_directory_path: Path, benchmark: NlSqlBenchmark, **kwargs) -> None:
    """
    Creates a context vector database for the specified database directory.

    Args:
        db_directory_path (str): The path to the database directory.
        **kwargs: Additional keyword arguments, including:
            - use_value_description (bool): Whether to include value descriptions (default is True).
    """
    docs = []

    benchmark.set_active_schema(db_id)
    schema = benchmark.get_active_schema()
    for table in schema.tables:
        for column in table.columns:
            metadata = {
                "table_name": table.name,
                "original_column_name": column.name,
                "column_name": column.name,
                "column_description": column.description if column.description != None else "",
                "value_description": column.value_description if column.value_description != None else ""
            }
            for key in ['name', 'description', 'value_description']:
                if column[key] != None:
                    docs.append(Document(page_content=column[key], metadata=metadata))
    
    logging.info(f"Creating context vector database for {db_id}")

    benchmark_path = db_directory_path / "context_vector_db"
    benchmark_path.mkdir(exist_ok=True)

    # vector_db_path = benchmark_path / f"{db_id}"

    # if vector_db_path.exists():
    #     if os.name == 'nt':  # Check if the OS is Windows
    #         os.system(f"rmdir /S /Q {vector_db_path}")
    #     else:  # For non-Windows systems
    #         os.system(f"rm -r {vector_db_path}")

    # vector_db_path.mkdir(exist_ok=True)

    Chroma.from_documents(docs, EMBEDDING_FUNCTION, persist_directory=str(benchmark_path))

    logging.info(f"Context vector database created at {benchmark_path}")



   
# Skalpel MOD: Retrieve unique values from benchmark object schema instead of database
def make_db_lsh(db_id: str, db_directory_path: Path, benchmark: NlSqlBenchmark, **kwargs) -> None:
    preprocessed_path = db_directory_path / "preprocessed" 
    preprocessed_path.mkdir(exist_ok=True)
    unique_values = {}
    schema = benchmark.get_active_schema(database=db_id)
    for table in tqdm(schema.tables, total=len(schema.tables), desc=f"Making DB LSH of {schema.database}"):
        unique_values[table.name] = {}
        for column in table.columns:
            if column.data_type.lower() not in ["string", "text", "clob", "ntext"] and "char" not in column.data_type.lower():
                continue
            if "." in column.name:
                continue
            col_vals = list(benchmark.get_unique_values(
                table_name=table.name,
                column_name=column.name,
                database=db_id
            ))
            if len(col_vals) > 0 and type(col_vals[0]) == str:
                unique_values[table.name][column.name] = col_vals
            
    logging.info("Unique values obtained")
    
    with open(preprocessed_path / f"{db_id}_unique_values.pkl", "wb") as file:
        pickle.dump(unique_values, file)
    logging.info("Saved unique values")
    lsh, minhashes = make_lsh(unique_values, **kwargs)
    
    with open(preprocessed_path / f"{db_id}_lsh.pkl", "wb") as file:
        pickle.dump(lsh, file)
    with open(preprocessed_path / f"{db_id}_minhashes.pkl", "wb") as file:
        pickle.dump(minhashes, file)

