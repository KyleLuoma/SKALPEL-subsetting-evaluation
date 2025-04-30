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
        self.preprocess_databases(
            filename_comments="on_init_not_for_performance_evaluation",
            skip_already_processed=True
            )
        self.ppl_lookup = self._load_ppl_file()



    def preprocess_databases(
            self, 
            exist_ok: bool = True, 
            filename_comments: str = "", 
            skip_already_processed: bool = False, 
            **args
            ) -> dict:
        # Construct `ppl_dev.json`
        ppl_path, ppl_processing_times = self._construct_ppl_dev(
            filename_comments=filename_comments, 
            skip_already_processed=skip_already_processed
            )
        # Construct few-shot examples pairs
        cqa.main()
        # Generate few-shot examples
        input_data = json.load(open(ppl_path, 'r'))
        if not skip_already_processed or not os.path.exists(
            f"src/SchemaSubsetter/RSLSQL/src/information/{self.benchmark.name}_example.json"
            ):
            sql_gen_processing_times = slg.run_sql_generation(
                input_data=input_data,
                out_file=f"src/SchemaSubsetter/RSLSQL/src/information/{self.benchmark.name}_example.json",
                k_shot=self.k_shot
            )
        with open(f"src/SchemaSubsetter/RSLSQL/src/information/{self.benchmark.name}_example.json", 'r') as src_file:
            with open("src/SchemaSubsetter/RSLSQL/src/information/example.json", 'w') as dest_file:
                dest_file.write(src_file.read())
        # add few-shot examples to ppl_dev.json
        self._add_example(ppl_path=ppl_path)

        processing_times = ppl_processing_times
        for db_name in processing_times.keys():
            processing_times[db_name] += sql_gen_processing_times[db_name]
            performance_time_file = f"./subsetting_results/preprocessing_times/{self.name}_{self.benchmark.name}_{db_name}_{filename_comments}_processing.json"
            with open(performance_time_file, "wt") as f:
                f.write(json.dumps({db_name: processing_times[db_name]}, indent=2))
        return processing_times


    def _construct_ppl_dev(self, filename_comments: str, skip_already_processed: bool) -> str:
        ppl_file_path = self.data_folder + f"/{self.benchmark.name}_ppl_dev.json"
        if skip_already_processed and os.path.exists(ppl_file_path):
            return ppl_file_path, {}
        bm_dev_list = []
        processing_times = {}
        for db in self.benchmark.databases:
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
            processing_times[db] = e_time - s_time

        temp_data_folder = self.data_folder + "/temp"
        os.makedirs(temp_data_folder, exist_ok=True)
        temp_file_path = os.path.join(self.data_folder, "temp.json")
        with open(temp_file_path, "w") as temp_file:
            json.dump(bm_dev_list, temp_file, indent=4)
        ppl_processing_times = dc.generate_ppl_dev_json(
            dev_file=temp_file_path,
            out_file=self.data_folder + f"/{self.benchmark.name}_ppl_dev.json"
        )
        for db_name in ppl_processing_times.keys():
            processing_times[db_name] += ppl_processing_times[db_name]
        return self.data_folder + f"/{self.benchmark.name}_ppl_dev.json", processing_times
    

    def _load_ppl_file(self) -> dict[tuple, dict]:
        ppl_file_path = self.data_folder + f"/{self.benchmark.name}_ppl_dev.json"
        if not os.path.exists(ppl_file_path):
            raise FileNotFoundError(ppl_file_path)
        with open(ppl_file_path, "rt") as f:
            ppl_list = json.loads(f.read())
        ppl_lookup = {}
        for ppl in ppl_list:
            ppl_lookup[(ppl["db"], ppl["question"])] = ppl
        return ppl_lookup
    

    def _add_example(self, ppl_path: str):
        """
        Integration of add_example.py
        """
        with open(ppl_path, 'r') as f:
            ppls = json.load(f)

        with open(f'src/SchemaSubsetter/RSLSQL/src/information/{self.benchmark.name}_example.json', 'r') as f:
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
        ppl = self.ppl_lookup[(benchmark_question.schema.database, benchmark_question.question)]
        # Forward schema linking: using LLM with a prompt to find tables and columns:
        table_info = step_1_preliminary_sql.table_info_construct(ppl)
        table_column_subset, tc_tokens = step_1_preliminary_sql.table_column_selection(table_info=table_info, ppl=ppl)
        information['tables'] = table_column_subset['tables']
        information['columns'] = table_column_subset['columns']
        # Then we generate a preliminary SQL statement
        # This is a modification of the RSSQL code because in the arxiv paper, preliminary sql is generated
        # by using the entire schema whereas the demonstration code uses the LLM-generated subset for
        # the preliminary sql.
        # table_column = {
        #     "tables": [table.name for table in benchmark_question.schema.tables],
        #     "columns": []
        # }
        # for table in benchmark_question.schema.tables:
        #     for column in table.columns:
        #         table_column["columns"].append(f"{table.name}.{column.name}")
        pre_sql, prelim_tokens = step_1_preliminary_sql.preliminary_sql(table_info=table_info, table_column=table_column_subset, ppl=ppl)
        # Backwards schema linking: Match identifiers in the question schema to identifiers in the sql statement:
        idents_from_sql = self._extract_from_sql(sql=pre_sql, question=benchmark_question)
        tables_in_subset: dict[str, SchemaTable] = {}
        schema_subset = Schema(
            database=benchmark_question.schema.database,
            tables=[]
            )
        # Add forward schema linking results to Schema subset
        for table in information["tables"]:
            if table not in tables_in_subset.keys():
                tables_in_subset[table] = SchemaTable(name=table, columns=[])
        for t_c in information["columns"]:
            table = t_c.split(".")[0]
            column = t_c.split(".")[1].replace("`", "")
            if table not in tables_in_subset.keys():
                tables_in_subset[table] = SchemaTable(name=table, columns=[TableColumn(name=column)])
            else:
                col_obj = TableColumn(name=column)
                if col_obj not in tables_in_subset[table].columns:
                    tables_in_subset[table].columns.append(col_obj)
        # Add backward schema linking results to Schema subset
        for t_c in idents_from_sql:
            table = t_c.split(".")[0]
            column = t_c.split(".")[1].replace("`", "")
            if table not in tables_in_subset.keys():
                tables_in_subset[table] = SchemaTable(name=table, columns=[TableColumn(name=column)])
            else:
                col_obj = TableColumn(name=column)
                if col_obj not in tables_in_subset[table].columns:
                    tables_in_subset[table].columns.append(col_obj)

        for table in tables_in_subset.keys():
            schema_subset.tables.append(tables_in_subset[table])

        return SchemaSubsetterResult(
            schema_subset=schema_subset,
            prompt_tokens=tc_tokens + prelim_tokens
        )


        
    def _extract_from_sql(self, sql: str, question: BenchmarkQuestion) -> list[str]:
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
        return pred_truth