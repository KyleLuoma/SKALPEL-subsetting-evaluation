"""
Wrapper class for the CHESS subsetter
https://github.com/ShayanTalaei/CHESS
"""

from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.SchemaSubsetterResult import SchemaSubsetterResult

from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from typing import List, Dict, Any

from SchemaSubsetter.CHESS.src import preprocess
from SchemaSubsetter.CHESS.src.workflow.system_state import SystemState
from SchemaSubsetter.CHESS.src.workflow.agents.information_retriever.information_retriever import InformationRetriever
from SchemaSubsetter.CHESS.src.runner.task import Task
from SchemaSubsetter.CHESS.src.runner.logger import Logger
from SchemaSubsetter.CHESS.src.runner.database_manager import DatabaseManager
from SchemaSubsetter.CHESS.src.workflow.team_builder import build_team

import time
import yaml
import json
import argparse
from pathlib import Path

from transformers import AutoTokenizer


class ChessSubsetter(SchemaSubsetter):

    name = "chess"

    def __init__(
            self,
            benchmark: NlSqlBenchmark,
            do_preprocessing: bool = False,
            overwrite_preprocessed: bool = False
    ):
        
        self.benchmark = benchmark
        self.db_root_directory = Path("./src/SchemaSubsetter/CHESS/data")
        self.result_directory = Path("./src/SchemaSubsetter/CHESS/results")

        parser = argparse.ArgumentParser()
        args = parser.parse_args()
        args.data_mode = 'dev'
        args.data_path = None
        args.config = "./src/SchemaSubsetter/CHESS/run/configs/CHESS_IR_SS.yaml"
        args.num_workers = 1
        args.log_level = 'warning'
        args.pick_final_sql = True
        args.n_gram=3
        args.threshold=0.01
        args.signature_size=100
        args.verbose=True
        self.args = args
        self.name = ChessSubsetter.name
        self.overwrite_preprocessed = overwrite_preprocessed

        with open(args.config, 'r') as file:
            self.config=yaml.safe_load(file)

        with open(f"./subsetting_results/preprocessing_times/chess_{benchmark.name}_processing.json", "wt") as f:
            json.dump({"processing_time": -1}, f)
        if do_preprocessing:
            s_time = time.perf_counter()
            self.preprocess_databases(exist_ok=self.overwrite_preprocessed)
            e_time = time.perf_counter()
            print("Time to process", self.benchmark.name, "schemas was", str(e_time - s_time))
            with open(f"./subsetting_results/preprocessing_times/chess_{benchmark.name}_processing.json", "wt") as f:
                json.dump({"processing_time_seconds": e_time - s_time}, f)
        self.team = build_team(self.config)


    def preprocess_databases(self, exist_ok: bool = True):

        for database in self.benchmark.databases:

            db_dir = self.db_root_directory / "dev_databases" / database

            try:
                db_dir.mkdir(exist_ok=exist_ok, parents=True)
            except OSError:
                continue

            preprocess.make_db_lsh(
                benchmark=self.benchmark,
                db_id=database,
                db_directory_path=db_dir,
                signature_size=self.args.signature_size,
                n_gram=self.args.n_gram,
                threshold=self.args.threshold,
                verbose=self.args.verbose
            )

            preprocess.make_db_context_vec_db(
                db_id=database,
                db_directory_path=db_dir,
                benchmark=self.benchmark
            )


    def get_schema_subset(self, benchmark_question: BenchmarkQuestion) -> SchemaSubsetterResult:
        """
        Replicates information retriever and schema selector agents in isolation from the full CHESS team context
        """
        team = build_team(self.config)
        task = Task(
            question_id=benchmark_question.question_number,
            db_id=benchmark_question.schema.database,
            question=benchmark_question.question,
            evidence="",
            SQL=benchmark_question.query,
            difficulty=None
        )
        DatabaseManager(db_mode=self.args.data_mode, db_id=task.db_id)
        logger = Logger(db_id=task.db_id, question_id=task.question_id, result_directory=self.result_directory)
        thread_id = f"{time.perf_counter()}_{task.db_id}_{task.question_id}"
        thread_config = {"configurable": {"thread_id": thread_id}}
        state_values = SystemState(
            task=task,
            tentative_schema=DatabaseManager().get_db_schema(),
            execution_history=[]
        )
        thread_config["recursion_limit"] = 50
        for state_dict in team.stream(state_values, thread_config, stream_mode="values"):
            logger.log("________________________________________________________________________________________")
            continue
        system_state = SystemState(**state_dict)
        schema_subset = Schema(
            database=benchmark_question.schema.database,
            tables=[
                SchemaTable(
                    name=table,
                    columns=[TableColumn(name=column) for column in system_state.tentative_schema[table]]
                ) 
                for table in system_state.tentative_schema
            ]
        )
        token_count, total_tokens = ChessSubsetter.get_token_counts_from_log(
            database_name=benchmark_question.schema.database,
            question_number=benchmark_question.question_number
            )
        return SchemaSubsetterResult(schema_subset=schema_subset, prompt_tokens=total_tokens)
    

    @staticmethod
    def get_token_counts_from_log(database_name: str, question_number: int) -> tuple[list[dict[str, Any]], int]:
        tokenizer = AutoTokenizer.from_pretrained("Xenova/gpt-4")
        logpath = "src/SchemaSubsetter/CHESS/results/logs"
        log_file = Path(logpath) / f"{question_number}_{database_name}.log"
        total_tokens = 0
        log_text = None
        try:
            with open(log_file, "rt", encoding="utf-8") as file:
                log_text = file.read()
        except UnicodeDecodeError as e:
            pass
        if log_text == None:
            try:
                with open(log_file, "rt", encoding="ascii") as file:
                    log_text = file.read()
            except Exception as e:
                return ({}, -1)
            
        entries = log_text.split("############################## Human at step ")
        token_counts = []
        for ix, entry in enumerate(entries):
            stage = entry.split(" ")[0].strip()
            input_prompt = entry.split("############################## AI at step ")[0]
            input_prompt = input_prompt.replace(stage, "")
            output = entry.split(f"############################## AI at step {stage} ##############################")
            if len(output) > 1:
                output = output[1].strip()
            else:
                output = output[0]
            token_counts.append({
                "ix": ix,
                "stage": stage,
                "input_token_count": len(tokenizer.encode(input_prompt)),
                "output_token_count": len(tokenizer.encode(output))
            })
            total_tokens += (len(tokenizer.encode(input_prompt)) + len(tokenizer.encode(output)))
        return (token_counts, total_tokens)
    

        
    