import os
from pathlib import Path
import logging
from dotenv import load_dotenv
from langchain_chroma import Chroma
from langchain.schema.document import Document
from langchain_openai import OpenAIEmbeddings


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
def make_db_context_vec_db(db_id: str, db_directory_path: str, benchmark: NlSqlBenchmark, **kwargs) -> None:
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

    benchmark_path = Path(db_directory_path) / benchmark.name
    benchmark_path.mkdir(exist_ok=True)

    vector_db_path = benchmark_path / f"{db_id}"

    if vector_db_path.exists():
        if os.name == 'nt':  # Check if the OS is Windows
            os.system(f"rmdir /S /Q {vector_db_path}")
        else:  # For non-Windows systems
            os.system(f"rm -r {vector_db_path}")

    vector_db_path.mkdir(exist_ok=True)

    Chroma.from_documents(docs, EMBEDDING_FUNCTION, persist_directory=str(vector_db_path))

    logging.info(f"Context vector database created at {vector_db_path}")
