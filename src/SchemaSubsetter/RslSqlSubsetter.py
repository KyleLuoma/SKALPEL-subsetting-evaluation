from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion

import json
import time

from SchemaSubsetter.RSLSQL.src import data_construct as dc
from SchemaSubsetter.RSLSQL.few_shot import (
    construct_QA as cqa,
    slg_main as slg
    )
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
            ):
        raise NotImplementedError
    
