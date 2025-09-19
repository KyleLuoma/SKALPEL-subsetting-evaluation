from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.SchemaSubsetterFactory import SchemaSubsetterFactory
from SubsetEvaluator.SchemaSubsetEvaluator import SchemaSubsetEvaluator
from SubsetEvaluator import QueryProfiler
from NlSqlEvaluator import NlSqlEvaluator
from util.StringObjectParser import StringObjectParser
import os
import time
import pandas as pd
from tqdm import tqdm
import pickle
import warnings
import json
import argparse
import logging

warnings.filterwarnings("ignore")

test = False

results_filename = "./subsetting_results/subsetting-CodeS-snails-NVIDIA_RTX_2000.xlsx"
results_filename = None

def main(cl_args, **kwargs):

    # logging.basicConfig(filename='./logs/main_runner.log', level=logging.INFO)

    def extract_sub_args(arg_string: str) -> dict:
        arg_dict = {k.split(":")[0]: k.split(":")[1] for k in arg_string.split("%")}
        for key in arg_dict:
            value: str = arg_dict[key]
            if value.isnumeric():
                arg_dict[key] = int(value)
            elif value.lower() in ["true", "false"]:
                arg_dict[key] = {"true": True, "false": False}[value.lower()]
            else:
                try:
                    fp_val = float(value)
                    arg_dict[key] = fp_val
                except ValueError as e:
                    pass
        return arg_dict

    results_filename = cl_args.results_filename
    subsetter_name = cl_args.subsetter_name
    benchmark_name = cl_args.benchmark_name
    recover_previous = cl_args.recover_previous
    filename_comments = cl_args.filename_comments
    cuda_device = cl_args.cuda_device
    verbose = cl_args.verbose
    subsetter_preprocessing = cl_args.subsetter_preprocessing
    subset_generation = not cl_args.no_subset_generation
    max_col_count = cl_args.max_col_count
    sleep_time = int(cl_args.sleep)
    nl_sql = cl_args.nl_sql
    if cl_args.subsetter_args:
        subsetter_args = extract_sub_args(cl_args.subsetter_args)
    else:
        subsetter_args = None
    if args.nlsql_args:
        nl_sql_args = extract_sub_args(cl_args.nlsql_args)
    else:
        nl_sql_args = {}

    global v_print
    if verbose:
        v_print = print
    else:
        def dummy(*values, **kwargs) -> None:
            pass
        v_print = dummy

    bm_factory = NlSqlBenchmarkFactory()
    benchmark = bm_factory.build_benchmark(benchmark_name)
    subsetter_factory = SchemaSubsetterFactory()
    if subsetter_args == None:
        subsetter_args = {}
    if cuda_device != None:
        subsetter_args["device"] = cuda_device
    subsetter = subsetter_factory.build_subsetter(
        subsetter_name=subsetter_name, 
        benchmark=benchmark, 
        subsetter_init_args=subsetter_args
        )
    if subsetter_preprocessing:
        s_time = time.perf_counter()
        db_processing_times = subsetter.preprocess_databases(
            exist_ok=False, 
            filename_comments=filename_comments,
            skip_already_processed=False,
            do_multiprocessing=False
            )
        e_time = time.perf_counter()
        total_time = e_time - s_time
        db_processing_times["total"] = total_time
        with open(f"./subsetting_results/preprocessing_times/{subsetter.name}_{benchmark.name}_{filename_comments}_processing.json", "wt") as f:
            f.write(json.dumps(db_processing_times, indent=2))

    if results_filename is None and subset_generation:
        results, subsets_questions = generate_subsets(
            subsetter=subsetter, 
            benchmark=benchmark,
            recover_previous=recover_previous,
            filename_comments=filename_comments,
            bypass_databases=[],
            max_col_count_to_generate=max_col_count,
            sleep=sleep_time
            )
        save_filename = f"./subsetting_results/subsetting-{subsetter.name}-{benchmark.name}-{benchmark.naturalness}-{filename_comments}.xlsx"
    elif results_filename is not None:
        results, subsets_questions = load_subsets_from_results(
            results_filename=results_filename, 
            benchmark=benchmark
            )
        save_filename = results_filename
        
    elif nl_sql is not None:
        do_nl_to_sql(
            result_file_substring=nl_sql, 
            recover_previous=recover_previous,
            **nl_sql_args
            )
        return

    else:
        print("Terminating program without generating or evaluating subsets.")
        return
    results = evaluate_subsets(subsets=subsets_questions, results=results)
    results_df = pd.DataFrame(results)

    end_while = False
    while not end_while:
        try:
            results_df.to_excel(
                save_filename,
                index=False
            )
            end_while = True
        except Exception as e:
            print("Could not save dataframe due to exception:")
            print(e)
            response = input("See if you can correct the issue and then press enter to try again. Enter quit to terminate without saving.")
            if response.lower() == "quit":
                end_while = True

        
    



def generate_subsets(
        subsetter: SchemaSubsetter, 
        benchmark: NlSqlBenchmark, 
        recover_previous: bool = False,
        filename_comments: str = "",
        bypass_databases: list = [],
        max_col_count_to_generate: int = None,
        sleep: int = 0
        ) -> tuple[dict, list]:
    results = {
        "database": [],
        "question_number": [],
        "query_filename": [],
        "inference_time": [],
        "prompt_tokens": []
    }
    failures: list[BenchmarkQuestion] = []
    test_counter = 0
    subsets_questions = []

    for question in tqdm(
        benchmark, 
        total=len(benchmark), 
        desc=f"Running {subsetter.name} subsetter over {benchmark.name} benchmark."
        ):

        test_counter += 1
        if question.schema.database in bypass_databases or (
            max_col_count_to_generate 
            and question.schema.get_column_count() > max_col_count_to_generate
            ):
            print(f"Bypassing database {question.schema.database} with {question.schema.get_column_count()} columns.")
            results["database"].append(question.schema.database)
            results["question_number"].append(question.question_number)
            results["query_filename"].append(question.query_filename)
            results["inference_time"].append(-1)
            results["prompt_tokens"].append(-1)
            subsets_questions.append((Schema(database=question.schema.database, tables=[]), question))
            continue

        recovery_filename = f"./.subsetting_recovery/{subsetter.name}/subsetting_recovery_{subsetter.name}_{benchmark.name}_{question.schema_naturalness}_{question.schema.database}_{question.question_number}_{filename_comments}.pkl"

        if recover_previous:
            try:
                with open(recovery_filename, "rb") as load_file:
                    loaded_result: dict = pickle.loads(load_file.read())
                    assert len(loaded_result["subset"].tables) > 0 # Prior subsetting failed
                    subsets_questions.append((loaded_result["subset"], loaded_result["question"]))
                    for key in results.keys():
                        results[key].append(loaded_result[key])
                continue
            except:
                pass

        v_print("\n", question["query"])
        v_print("--- Running subsetter ---")
        t_start = time.perf_counter()
        try:
            result = subsetter.get_schema_subset(
                benchmark_question=question
            )
            subset = result.schema_subset
        except UnboundLocalError as e:
            failures.append((question, str(e)))
        if result.error_message is not None:
            failures.append((question, f"Error: {result.error_message}\nPrompt: {str(result.raw_llm_response)}"))
        t_end = time.perf_counter()
        v_print("Subsetting time:", str(t_end - t_start))
        time.sleep(sleep)
        subsets_questions.append((subset, question))
        results["database"].append(question["schema"]["database"])
        results["question_number"].append(question["question_number"])
        if question.query_filename is not None:
            results["query_filename"].append(question.query_filename)
        else:
            results["query_filename"].append("")
        results["inference_time"].append(t_end - t_start)
        results["prompt_tokens"].append(result.prompt_tokens)
        v_print(subset)
        if test and test_counter > 5:
            break

        recovery_dir = os.path.dirname(recovery_filename)
        if not os.path.exists(recovery_dir):
            os.makedirs(recovery_dir, exist_ok=True)
        with open(recovery_filename, "wb") as recovery_file:
            save_results = {}
            for key in results.keys():
                save_results[key] = results[key][-1]
            save_results["subset"] = subset
            save_results["question"] = question
            pickle.dump(save_results, recovery_file)

    if len(failures) > 0:
        with open(f"./subset_failures_{subsetter.name}_{benchmark.name}.log", "wt", encoding="utf-8") as f:
            for failure in failures:
                e = failure[1]
                q: BenchmarkQuestion = failure[0]
                f.write(f"##### {q.schema.database} - {q.question_number} #####\n")
                f.write(e)
                f.write("\n  ## Question:\n")
                f.write(f"    {q.question}\n")
                f.write("\n  ## Query:\n")
                f.write(f"    {q.query}\n\n\n")

    return results, subsets_questions



def evaluate_subsets(subsets: list, results: dict) -> dict:
    evaluator = SchemaSubsetEvaluator(use_result_cache=False) 
    for s_q_pair in tqdm(subsets, desc=f"Evaluating the subsets"):
        subset = s_q_pair[0]
        question = s_q_pair[1]
        v_print("--- Running evaluator ---")
        ev_t_start = time.perf_counter()
        scores = evaluator.evaluate_schema_subset(
            predicted_schema_subset=subset,
            question=question
        )
        ev_t_end = time.perf_counter()
        v_print("Evaluation time:", str(ev_t_end - ev_t_start))
        for k in scores.keys():
            if k not in results.keys():
                results[k] = []
            results[k].append(scores[k])
        v_print(scores)
    return results



def load_subsets_from_results(results_filename: str, benchmark: NlSqlBenchmark) -> tuple[dict, list]:
    results_df = pd.read_excel(results_filename)
    results = {
        "database": [],
        "question_number": [],
        "query_filename": [],
        "inference_time": [],
        "prompt_tokens": []
    }
    subsets_questions = []
    for row in results_df.itertuples():
        if benchmark.get_active_schema().database != row.database:
            benchmark.set_active_schema(row.database)
        benchmark.set_active_question_number(row.question_number)
        question = benchmark.get_active_question()
        results["database"].append(row.database)
        results["question_number"].append(row.question_number)
        try:
            results["query_filename"].append(row.query_filename)
        except:
            results["query_filename"].append("")
        results["inference_time"].append(row.inference_time)
        results["prompt_tokens"].append(row.prompt_tokens)
        correct_tables = StringObjectParser.string_to_python_object(row.correct_tables)
        correct_columns = StringObjectParser.string_to_python_object(row.correct_columns)
        extra_tables = StringObjectParser.string_to_python_object(row.extra_tables)
        if type(extra_tables) == str and extra_tables[0] == "{":
            extra_tables = set([t.strip().replace("'", "") for t in extra_tables[1:].split(",")])
        extra_columns = StringObjectParser.string_to_python_object(row.extra_columns)
        if type(extra_columns) == str and extra_columns[0] == "{":
            extra_columns = set([c.strip().replace("'", "") for c in extra_columns[1:].split(",")])

        table_items = []
        for table_name in correct_tables.union(extra_tables):
            column_items = []
            for table_column in correct_columns.union(extra_columns):
                column_table = ".".join(table_column.split(".")[:-1])
                column_name = table_column.split(".")[-1]
                if column_table != table_name:
                    continue
                table_column = TableColumn(name=column_name, data_type="")
                column_items.append(table_column)
            table_items.append(SchemaTable(
                name=table_name,
                columns=column_items,
                primary_keys=[],
                foreign_keys=[]
                ))
        if "SBOD" in question.schema.database:
            pause = True
        schema_subset = Schema(database=row.database, tables=table_items)
        subsets_questions.append((schema_subset, question))
    return results, subsets_questions



def do_nl_to_sql(
        result_file_substring: str, 
        recover_previous: bool = False, 
        full_schema: bool = False,
        model: str = None,
        comment_model_filter: bool = True
        ):
    
    if model == None:
        model = NlSqlEvaluator.LLM.OpenAIRequestLLM.DEFAULT_MODEL
    completed_nl_to_sql_files = [
        f for f in os.listdir("./nl_sql_results")
        if f.endswith(".xlsx")
    ]
    subset_filenames = [
        f for f in os.listdir("./subsetting_results")
        if f.endswith(".xlsx") and result_file_substring in f
    ]
    if comment_model_filter:
        filter_map = {
            "CodeS": "lambda1_sic_merged",
            "DINSQL": "gpt41",
            "chess": "gpt4o",
            "crush4sql": "lambda1",
            "dtssql": "lambda1",
            "rslsql": "gpt41",
            "tasql": "gpt41",
            "skalpel": "vector_qdecomp_525th",
            "skalpeltasql": "gpt41nano-vectorsort",
            "perfect_subsetter": "oracle",
            "perfect_table_subsetter": "oracle"
        }
        filtered_filenames = set()
        for fn in subset_filenames:
            fn_model = fn.split("-")[1]
            fn_comment = fn.split("-")[4].replace(".xlsx", "")
            if filter_map[fn_model] == fn_comment:
                filtered_filenames.add(fn)
        subset_filenames = list(filtered_filenames)
    benchmark_factory = NlSqlBenchmarkFactory()
    if full_schema:
        for bm_name in benchmark_factory.benchmark_register:
            print(f"NL to SQL over full schema for benchmark: {bm_name}")
            evaluator = NlSqlEvaluator.NlSqlEvaluator(benchmark=benchmark_factory.build_benchmark(bm_name))
            nl_sql_results = evaluator.generate_sql_from_subset_df_or_benchmark(
                subset_df=None,
                llm_model=model,
                source_filename="fullschema",
                recover_previous=recover_previous
            )
            nl_sql_filename = f"nltosql-nosubset-{bm_name}-Native-fullschema-nlsqlmodel_{model.replace('/', '-')}.xlsx"
            nl_sql_results.to_excel(f"./nl_sql_results/{nl_sql_filename}")
    counter = 1
    for filename in subset_filenames:
        print(f"NL to SQL over: {filename} ({counter} of {len(subset_filenames)})")
        counter += 1
        nl_sql_filename = filename.replace("subsetting-", "nltosql-")
        nl_sql_filename = nl_sql_filename.replace(".xlsx", f"-nlsqlmodel_{model.replace('/', '-')}.xlsx")
        if nl_sql_filename in completed_nl_to_sql_files:
            continue
        v_print(filename)
        benchmark_name = filename.split("-")[2]
        evaluator = NlSqlEvaluator.NlSqlEvaluator(benchmark=benchmark_factory.build_benchmark(benchmark_name))
        nl_sql_results = evaluator.generate_sql_from_subset_df_or_benchmark(
            subset_df=pd.read_excel(f"./subsetting_results/{filename}"),
            source_filename=filename,
            recover_previous=recover_previous,
            llm_model=model
        )
        nl_sql_results.to_excel(f"./nl_sql_results/{nl_sql_filename}")
        



    
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run schema subsetting evaluation.")
    parser.add_argument("--subsetter_name", type=str, default="abstract", help="Name of the subsetter to use.")
    parser.add_argument("--benchmark_name", type=str, default="snails", help="Name of the benchmark to use.")
    parser.add_argument("--recover_previous", action="store_true", default=False, help="Retrieve partial subsetting results for the provided configuration (Usefull if the process was interupted.)")
    parser.add_argument("--filename_comments", type=str, default="", help="Comments to append to the filename.")
    parser.add_argument("--cuda_device", type=int, default=None, help="CUDA device to use.")
    parser.add_argument("--verbose", action="store_true", default=False, help="Enable verbose output.")
    parser.add_argument("--subsetter_preprocessing", action="store_true", default=False, help="Enable subsetter preprocessing.")
    parser.add_argument("--no_subset_generation", action="store_true", default=False, help="Set to True to skip subsetting (e.g., if you want to preprocess only).")
    parser.add_argument("--max_col_count", type=int, default=None, help="Limit subset generation to schemas with fewer than this number of columns.")
    parser.add_argument("--results_filename", default=None, help="Load specified results and evaluate only (no re-subsetting).")
    parser.add_argument("--sleep", default=0, help="Time (seconds) to sleep between subsetting inferences.")
    parser.add_argument("--nl_sql", type=str, default=None, help="Run nl-to-sql over xlsx files in /subsetting_results. Use the argument to filter files to run. Argument is a substring of the filename. E.g., 'subsetting-tasql-bird' will run nl-to-sql on all filenames containing that substring")
    parser.add_argument("--subsetter_args", type=str, default=None, help="Subsetter specific arguments, k:v ^ delimited. Example model:gpt-4.1%vector_threshold:0.575")
    parser.add_argument("--nlsql_args", type=str, default=None, help="NL-to-SQL specific arguments, k:v % delimiter")
    args = parser.parse_args()

    # if args.nl_sql is None:
    #     snails_xlsx_files = [
    #         f for f in os.listdir("./subsetting_results")
    #         if f.endswith(".xlsx") and "snails" in f
    #     ]
    #     for f in snails_xlsx_files:
    #         print(f)
    #         args = parser.parse_args()
    #         args.results_filename = "./subsetting_results/" + f
    #         main(args)
    # else:
    main(args)