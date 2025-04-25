from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.SchemaSubsetterResult import SchemaSubsetterResult
from NlSqlBenchmark.SchemaObjects import (
    Schema, SchemaTable, TableColumn, ForeignKey
)
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion

import json
import time

from SchemaSubsetter.RSLSQL.src import data_construct as dc
from SchemaSubsetter.RSLSQL.few_shot import (
    construct_QA as cqa,
    slg_main as slg
    )
from SchemaSubsetter.RSLSQL.src import step_1_preliminary_sql
from SchemaSubsetter.RSLSQL.src import bid_schema_linking
import os

class RslSqlSubsetter(SchemaSubsetter):
    """
    Skalpel subsetter wrapper for RSL-SQL (https://github.com/Laqcce-cao/RSL-SQL)
    """
    name = "rslsql"

    def __init__(self, benchmark: NlSqlBenchmark):
        self.name = RslSqlSubsetter.name
        self.benchmark = benchmark
        self.data_folder = "src/SchemaSubsetter/RSLSQL/data"
        self.k_shot = 3
        self.preprocess_databases(filename_comments="on_init_not_for_performance_evaluation")


    def preprocess_databases(
            self, 
            exist_ok: bool = True, 
            filename_comments: str = "", 
            skip_already_processed: bool = False, 
            **args
            ):
        # Construct `ppl_dev.json`
        ppl_path, processing_times = self._construct_ppl_dev(filename_comments=filename_comments)
        # Construct few-shot examples pairs
        cqa.main()
        # Generate few-shot examples
        input_data = json.load(open(ppl_path, 'r'))
        slg.run_sql_generation(
            input_data=input_data,
            out_file="src/SchemaSubsetter/RSLSQL/src/information/example.json",
            k_shot=self.k_shot
        )
        # add few-shot examples to ppl_dev.json
        self._add_example(ppl_path=ppl_path)
        return processing_times




    def _construct_ppl_dev(self, filename_comments: str) -> str:
        bm_dev_list = []
        processing_times = {}
        for db in self.benchmark.databases:
            performance_time_file = f"./subsetting_results/preprocessing_times/{self.name}_{self.benchmark.name}_{db}_{filename_comments}_processing.json"
            s_time = time.perf_counter()
            for question in self.benchmark:
                bm_dev_list.append({
                    "question_id": question.question_number,
                    "db_id": question.schema.database,
                    "question": question.question,
                    "evidence": "",
                    "SQL": question.query,
                    "difficulty": ""
                })
            e_time = time.perf_counter()
            with open(performance_time_file, "wt") as f:
                f.write(json.dumps({db: e_time - s_time}, indent=2))
            processing_times[db] = e_time - s_time

        temp_data_folder = self.data_folder + "/temp"
        os.makedirs(temp_data_folder, exist_ok=True)
        temp_file_path = os.path.join(self.data_folder, "temp.json")
        with open(temp_file_path, "w") as temp_file:
            json.dump(bm_dev_list, temp_file, indent=4)
        dc.generate_ppl_dev_json(
            dev_file=temp_file_path,
            out_file=self.data_folder + f"/{self.benchmark.name}_ppl_dev.json"
        )
        return self.data_folder + f"/{self.benchmark.name}_ppl_dev.json", processing_times
    

    def _add_example(self, ppl_path: str):
        """
        Integration of add_example.py
        """
        with open(ppl_path, 'r') as f:
            ppls = json.load(f)

        with open('src/SchemaSubsetter/RSLSQL/src/information/example.json', 'r') as f:
            examples = json.load(f)

        for i in range(len(ppls)):
            ppls[i]['example'] = examples[i]

        with open(ppl_path, 'w') as f:
            json.dump(ppls, f, indent=4, ensure_ascii=False)


    def get_schema_subset(
            self, 
            benchmark_question: BenchmarkQuestion
            ) -> SchemaSubsetterResult:
        
        information = {}
        ppl = {
            "question_id": benchmark_question.question_number,
            "db_id": benchmark_question.schema.database,
            "question": benchmark_question.question,
            "evidence": "",
            "SQL": benchmark_question.query,
            "difficulty": ""
        }
        # Forward schema linking: using LLM with a prompt to find tables and columns:
        table_info = step_1_preliminary_sql.table_info_construct(ppl)
        table_column_subset = step_1_preliminary_sql.table_column_selection(table_info=table_info, ppl=ppl)
        information['tables'] = table_column_subset['tables']
        information['columns'] = table_column_subset['columns']
        # Then we generate a preliminary SQL statement
        # This is a modification of the RSSQL code because in the arxiv paper, preliminary sql is generated
        # by using the entire schema whereas the demonstration code uses the LLM-generated subset for
        # the preliminary sql.
        table_column = {
            "tables": [table.name for table in benchmark_question.schema.tables],
            "columns": []
        }
        for table in benchmark_question.schema.tables:
            for column in table.columns:
                table_column["columns"].append(f"{table.name}.{column.name}")
        pre_sql = step_1_preliminary_sql.preliminary_sql(table_info=table_info, table_column=table_column, ppl=ppl)
        # Backwards schema linking: Match identifiers in the question schema to identifiers in the sql statement:
        idents_from_sql = self._extract_from_sql(sql=pre_sql, question=benchmark_question)


        
    def _extract_from_sql(self, sql: str, question: BenchmarkQuestion) -> list:
        """
        Derived from RSLSQL.src.bid_schema_linking.extract_from_sql method 
        by replicating the string matching functionality of the original method.
        """
        pred_truth = []
        sql = sql.lower()
        for table in question.schema.tables:
            if table.name.lower() == "sqlite_sequence":
                continue
            for column in table.columns:
                if column.name.lower() in sql:
                    pred_truth.append(f"{table.name}.{column.name}")