from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark

from SchemaSubsetter.CRUSH4SQL.demo.utils.utils import ndap_pipeline, get_openai_embedding

import json
from pathlib import Path
import torch

class Crush4SqlSubsetter(SchemaSubsetter):

    name = "crush4sql"

    def __init__(self, benchmark: NlSqlBenchmark):
        self.name = Crush4SqlSubsetter.name
        self.benchmark = benchmark
        self.processed_db_dir = Path("./src/SchemaSubsetter/CRUSH4SQL/processed")
        self.correct_txt_sql_pairs = {}
        self.api_type = 'python'
        with open("./.local/openai.json") as f:
            open_ai_info = json.load(f)
        self.api_key = open_ai_info["api_key"]
        self.endpoint = None
        self.api_version = None
        self.table_code_lookup = {}
        self.code_table_lookup = {}
        self.next_code = 1000


    def get_schema_subset(self, benchmark_question: BenchmarkQuestion) -> Schema:
        hallucinated_schema, predicted_schema, predicted_sql = ndap_pipeline(
                    benchmark_question.question, 
                    self.api_type, 
                    self.api_key, 
                    self.endpoint, 
                    self.api_version, 
                    self.correct_txt_sql_pairs,
                    generate_sql_query=False
                )
        

    def preprocess_databases(self):
        for db in self.benchmark.databases:
            processed_path = self.processed_db_dir / schema.database
            processed_path.mkdir(exist_ok=False)

            schema = self.benchmark.get_active_schema(database=db)
            flattened_schema = self._flatten_schema(schema=schema)
            with open(processed_path / f"{db}_super_flat_unclean.txt", "w") as file:
                file.write("\n".join(flattened_schema))

            relation_map = self._make_relation_map(schema=schema)
            with open(processed_path / f"{db}_relation_map_for_unclean.json", "wt") as file:
                file.write(json.dump(relation_map))

            embeddings = []            
            for document in flattened_schema:
                embedding = get_openai_embedding(
                    query=document,
                    api_type=self.api_type,
                    api_key=self.api_key,
                    endpoint=self.endpoint,
                    api_version=self.api_version
                )
                embeddings.append(embedding)
            with open(processed_path / f"{db}_openai_docs_unclean_embedding.pickle", "wb") as file:
                torch.save(torch.Tensor(embeddings), file)


    def _register_table(self, db_table_name: str) -> int:
        table_code = self.next_code
        if db_table_name not in self.table_code_lookup.keys():
            self.table_code_lookup[db_table_name] = table_code
            self.code_table_lookup[table_code] = db_table_name
            self.next_code += 1
        else:
            table_code = self.table_code_lookup[db_table_name]
        return table_code


    def _flatten_schema(self, schema: Schema) -> list[str]:
        flattened_schema = []
        for table in schema.tables:
            db_table_name = f"{schema.database}.{table.name}"
            table_code = self._register_table(db_table_name=db_table_name)
            for column in table.columns:
                flattened_schema.append(f"{schema.database} {table.name} {table_code} {column.name}")
        return flattened_schema


    def _make_relation_map(self, schema: Schema) -> dict[dict[str, str]]:
        relation_map = {}
        for table in schema.tables:
            db_table_name = f"{schema.database}.{table.name}"
            table_code = self._register_table(db_table_name=db_table_name)
            for column in table.columns:
                key = f"{schema.database} {table.name} {table_code} {column.name}"
                relation_map[key] = {
                    "table": table.name,
                    "source": schema.database,
                    "code": str(table_code)
                }
        return relation_map