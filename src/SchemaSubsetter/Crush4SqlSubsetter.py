from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.SchemaSubsetterResult import SchemaSubsetterResult
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark

from SchemaSubsetter.CRUSH4SQL.demo.utils.utils import (
    get_openai_embedding, get_hallucinated_segments, clean_segments
)
from SchemaSubsetter.CRUSH4SQL.demo.utils.score import get_scored_docs
from SchemaSubsetter.CRUSH4SQL.demo.utils.greedy_selection import greedy_select
from SchemaSubsetter.CRUSH4SQL.demo.utils.sql_utils import create_schema

import json
from pathlib import Path
import torch
from tqdm import tqdm
import time

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
        self.super_schemas = {}
        self._register_all_tables()
        self.relation_maps = self._load_benchmark_relation_maps(self.benchmark.databases)


    def get_schema_subset(self, benchmark_question: BenchmarkQuestion) -> SchemaSubsetterResult:
        decomposition_prompt_used = 'hallucinate_schema_ndap'
        hallucinated_schema, hallucination_tokens = get_hallucinated_segments(
            prompt_type=decomposition_prompt_used,
            query=benchmark_question.question,
            api_type=self.api_type,
            api_key=self.api_key,
            endpoint=self.endpoint,
            api_version=""
        )
        hallucinated_schema = [x for x in hallucinated_schema if len(x) > 0]
        if self.relation_maps == None:
            self._load_benchmark_relation_maps(self.benchmark.databases)
        segments = clean_segments(hallucinated_schema)
        processed_filepath = f"./src/SchemaSubsetter/CRUSH4SQL/processed/{benchmark_question.schema.database}"
        scored_docs = get_scored_docs(
            question=benchmark_question.question,
            segments=segments,
            api_type=self.api_type,
            api_key=self.api_key,
            endpoint=None,
            api_version=self.api_version,
            processed_benchmark_filepath=processed_filepath,
            database_name=benchmark_question.schema.database
        )
        global RELATION_MAP
        RELATION_MAP = self.relation_maps[benchmark_question.schema.database]
        greedy_docs = greedy_select(
            segments=segments, 
            docs=scored_docs, 
            BUDGET=20,
            skalpel_relation_map=RELATION_MAP
            )

        sql_input = {
            'question': benchmark_question.question,
            'docs': greedy_docs,
        }
        # We do this because we don't provide correct text-to-sql pairs for subsetting
        prompting_type = "base"
        fewshot_examples = []
        if benchmark_question.schema.database not in self.super_schemas.keys():
            self.super_schemas[benchmark_question.schema.database] = self._make_super_schema(benchmark_question.schema)
        pred_schema = create_schema(
            sql_input["docs"], 
            schema_meta=self.super_schemas[benchmark_question.schema.database],
            local_relation_map=RELATION_MAP
            )
        schema_subset = Schema(database=benchmark_question.schema.database, tables=[])
        for table in pred_schema.keys():
            subset_table = table.split(" ")[0]
            schema_subset.tables.append(SchemaTable(
                name=subset_table,
                columns=[TableColumn(name=c) for c in pred_schema[table]]
            ))
        result = SchemaSubsetterResult(
            schema_subset=schema_subset,
            prompt_tokens=hallucination_tokens
        )
        return result

        
         

    def preprocess_databases(
            self, 
            exist_ok: bool = True, 
            filename_comments: str = "",
            skip_already_processed: bool = False
            ) -> dict[str, float]:
        processing_times = {}
        for db in self.benchmark.databases:
            performance_time_file = f"./subsetting_results/preprocessing_times/{self.name}_{self.benchmark.name}_{db}_{filename_comments}_processing.json"
            if skip_already_processed and Path(performance_time_file).exists():
                print(f"Skipping {db} as processing time file already exists.")
                continue

            print("Crush4Sql preprocessing", db)
            s_time = time.perf_counter()
            schema = self.benchmark.get_active_schema(database=db)

            processed_path = self.processed_db_dir / schema.database
            try:
                processed_path.mkdir(exist_ok=exist_ok, parents=True)
            except FileExistsError as e:
                passd

            flattened_schema = self._flatten_schema(schema=schema)
            with open(processed_path / f"{db}_super_flat_unclean.txt", "w") as file:
                file.write("\n".join(flattened_schema))

            relation_map = self._make_relation_map(schema=schema)
            with open(processed_path / f"{db}_relation_map_for_unclean.json", "wt") as file:
                file.write("\n" + json.dumps(relation_map, indent=2))

            embeddings = []            
            for document in tqdm(flattened_schema, desc="Generating embeddings"):
                embedding = get_openai_embedding(
                    query=document,
                    api_type=self.api_type,
                    api_key=self.api_key,
                    endpoint=self.endpoint,
                    api_version=self.api_version
                )
                embeddings.append(embedding)
            stacked_embeddings = torch.stack(embeddings)
            with open(processed_path / f"{db}_openai_docs_unclean_embedding.pickle", "wb") as file:
                # for embedding in embeddings:
                torch.save(stacked_embeddings, file)
            e_time = time.perf_counter()
            processing_times[db] = e_time - s_time

            with open(performance_time_file, "wt") as f:
                f.write(json.dumps({db: e_time - s_time}, indent=2))

        return processing_times


    def _register_all_tables(self):
        for db in self.benchmark.databases:
            schema = self.benchmark.get_active_schema(db)
            for table in schema.tables:
                self._register_table(f"{db}.{table.name}")


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
    

    def _make_super_schema(self, schema: Schema):
        super_sch = {}
        for table in schema.tables:
            table_entry = {
                "name": table.name,
                "overview": "",
                "source": schema.database,
                "columns": [c.name for c in table.columns],
                "key_columns": table.primary_keys
            }
            super_sch[self.table_code_lookup[f"{schema.database}.{table.name}"]] = table_entry
        return super_sch
    

    def _load_benchmark_relation_maps(self, benchmark_databases: list):
        database_relation_maps = {}
        for db in benchmark_databases:
            try:
                database_relation_maps[db] = json.loads(
                    open(f"./src/SchemaSubsetter/CRUSH4SQL/processed/{db}/{db}_relation_map_for_unclean.json").read()
                    )
            except FileNotFoundError as e:
                pass
        return database_relation_maps