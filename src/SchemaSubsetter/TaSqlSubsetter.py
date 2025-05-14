from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.SchemaSubsetterResult import SchemaSubsetterResult
from NlSqlBenchmark.SchemaObjects import (
    Schema, SchemaTable, TableColumn, ForeignKey
)
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion

from SchemaSubsetter.TASQL.src import conclude_meaning
from SchemaSubsetter.TASQL.src.modules import TASL

import time
import os
import json
import multiprocessing as mp
from pathlib import Path


class TaSqlSubsetter(SchemaSubsetter):

    name = "tasql"

    def __init__(self, benchmark: NlSqlBenchmark, column_describe_model: str = "gpt-4.1-nano"):
        self.benchmark = benchmark
        self.column_meaning_path = Path(f"./src/SchemaSubsetter/TASQL/outputs/{benchmark.name}")
        self.dummy_db_root_path = Path(f"./src/SchemaSubsetter/TASQL/data/{benchmark.name}")
        self.column_describe_model = column_describe_model
        self.question_id_lookup: dict[tuple[str, int], int] = {}
        for id, question in enumerate(benchmark):
            self.question_id_lookup[(question.schema.database, question.question_number)] = id

    def preprocess_databases(
            self, 
            exist_ok: bool = True, 
            filename_comments: str = "", 
            skip_already_processed: bool = False, 
            do_multiprocessing: bool = False,
            **args
            ) -> dict[str, float]:
        try:
            self.column_meaning_path.mkdir(exist_ok=exist_ok)
        except FileExistsError as e:
            pass
        try:
            self.dummy_db_root_path.mkdir(exist_ok=exist_ok)
        except FileExistsError as e:
            pass

        prompt_dic, prompt_make_times = conclude_meaning.get_prompts_skalpel(self.benchmark)

        if not os.path.exists(self.column_meaning_path / "column_meaning.json") and not skip_already_processed:

            if do_multiprocessing:

                num_processors = min(128, os.cpu_count() or 1)
                split_prompt_dics = [{} for i in range(num_processors)]
                for i, k in enumerate(prompt_dic.keys()):
                    split_prompt_dics[i % num_processors][k] = prompt_dic[k]

                with mp.Pool(processes=num_processors) as pool:
                    results = [
                        pool.apply_async(
                            conclude_meaning.conclude_each_column,
                            args=(
                                split_prompt_dics[i],
                                str(self.column_meaning_path / f"column_meaning_{i}.json")
                            ),
                            kwds={"skip_already_processed": skip_already_processed},
                        )
                        for i in range(num_processors)
                    ]
                    conclude_columns_times_list = [result.get() for result in results]

                conclude_columns_times = {}
                for times in conclude_columns_times_list:
                    conclude_columns_times.update(times)

            else:
                conclude_columns_times = conclude_meaning.conclude_each_column(
                    prompt_dic, 
                    str(self.column_meaning_path / "column_meaning.json"),
                    skip_already_processed=skip_already_processed
                    )
        else:
            conclude_columns_times = {db: 0 for db in prompt_make_times.keys()}

        processing_times = {}
        if not skip_already_processed:
            for db in prompt_make_times.keys():
                processing_times[db] = prompt_make_times[db] + conclude_columns_times[db]
                performance_time_file = f"./subsetting_results/preprocessing_times/{self.name}_{self.benchmark.name}_{db}_{filename_comments}_processing.json"
                with open(performance_time_file, "wt") as f:
                    f.write(json.dumps({db: processing_times[db]}, indent=2))

        self._make_table_json()
        self._make_question_json()

        return processing_times
    

    def _make_table_json(self):
        table_json = []
        for db in self.benchmark.databases:
            table_json.append(self.benchmark.get_active_schema(db).as_bird_json_format())

        json.dump(table_json, open(self.dummy_db_root_path / "dev_tables.json", "w"), indent=4)


    def _make_question_json(self):
        question_json = []
        for question in self.benchmark:
            question_json.append({
                "question_id": self.question_id_lookup[(question.schema.database, question.question_number)],
                "db_id": question.schema.database,
                "question": question.question,
                "evidence": "",
                "SQL": question.query,
                "difficulty": "ggg"
            })
        json.dump(question_json, open(self.dummy_db_root_path / "dev.json", "w"), indent=4)


    def get_schema_subset(
            self, 
            benchmark_question: BenchmarkQuestion
            ) -> SchemaSubsetterResult:
        tasl = TASL(
            db_root_path=self.dummy_db_root_path,
            mode="dev",
            column_meaning_path=str(self.column_meaning_path / "column_meaning.json")
        )
        schema, token_count = tasl.get_schema(question_id=self.question_id_lookup[(
            benchmark_question.schema.database,
            benchmark_question.question_number
        )])
        added_tables: dict[str, SchemaTable] = {}
        for table_column in schema:
            table = table_column[0]
            column = table_column[1]
            if table not in added_tables.keys():
                added_tables[table] = SchemaTable(
                    name=table,
                    columns=[TableColumn(name=column)]
                )
            else:
                added_tables[table].columns.append(
                    TableColumn(name=column)
                )
        skalpel_schema = Schema(
            database=benchmark_question.schema.database,
            tables=[table for table in added_tables.values()]
        )
        return SchemaSubsetterResult(
            schema_subset=skalpel_schema,
            prompt_tokens=token_count 
        )
        