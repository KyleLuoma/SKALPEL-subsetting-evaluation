"""
Wrapper class for the CHESS subsetter
https://github.com/ShayanTalaei/CHESS
"""

from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
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
import argparse
from pathlib import Path


class ChessSubsetter(SchemaSubsetter):

    def __init__(
            self,
            benchmark: NlSqlBenchmark,
            do_preprocessing: bool = False
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

        with open(args.config, 'r') as file:
            self.config=yaml.safe_load(file)

        if do_preprocessing:
            s_time = time.perf_counter()
            self.preprocess_databases()
            e_time = time.perf_counter()
            print("Time to process", self.benchmark.name, "schemas was", str(e_time - s_time))
        self.team = build_team(self.config)


    def preprocess_databases(self):

        for database in self.benchmark.databases:

            db_dir = self.db_root_directory / "dev_databases" / database
            db_dir.mkdir(exist_ok=True, parents=True)

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


    def get_schema_subset(self, benchmark_question: BenchmarkQuestion) -> Schema:
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


 
        
    