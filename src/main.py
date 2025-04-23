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
from util.StringObjectParser import StringObjectParser
import os
import time
import pandas as pd
from tqdm import tqdm
import pickle
import warnings
import json
import argparse

warnings.filterwarnings("ignore")

test = False

results_filename = "./subsetting_results/subsetting-CodeS-snails-NVIDIA_RTX_2000.xlsx"
results_filename = None

def main():

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

    args = parser.parse_args()

    subsetter_name = args.subsetter_name
    benchmark_name = args.benchmark_name
    recover_previous = args.recover_previous
    filename_comments = args.filename_comments
    cuda_device = args.cuda_device
    verbose = args.verbose
    subsetter_preprocessing = args.subsetter_preprocessing
    subset_generation = not args.no_subset_generation
    max_col_count = args.max_col_count
    

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
            skip_already_processed=True
            )
        e_time = time.perf_counter()
        total_time = e_time - s_time
        db_processing_times["total"] = total_time
        with open(f"./subsetting_results/preprocessing_times/{subsetter.name}_{benchmark.name}_{filename_comments}_processing.json", "wt") as f:
            f.write(json.dumps(db_processing_times, indent=2))

    if results_filename == None and subset_generation:
        results, subsets_questions = generate_subsets(
            subsetter=subsetter, 
            benchmark=benchmark,
            recover_previous=recover_previous,
            filename_comments=filename_comments,
            bypass_databases=[],
            max_col_count_to_generate=max_col_count
            )
    elif results_filename != None:
        results, subsets_questions = load_subsets_from_results(
            results_filename=results_filename, 
            benchmark=benchmark
            )
    else:
        print("Terminating program without generating or evaluating subsets.")
        return
    results = evaluate_subsets(subsets=subsets_questions, results=results)
    results_df = pd.DataFrame(results)

    end_while = False
    while not end_while:
        try:
            results_df.to_excel(
                f"./subsetting_results/subsetting-{subsetter.name}-{benchmark.name}-{benchmark.naturalness}-{filename_comments}.xlsx",
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
        max_col_count_to_generate: int = None
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
        if result.error_message != None:
            failures.append((question, f"Error: {result.error_message}\nPrompt: {str(result.raw_llm_response)}"))
        t_end = time.perf_counter()
        v_print("Subsetting time:", str(t_end - t_start))
        subsets_questions.append((subset, question))
        results["database"].append(question["schema"]["database"])
        results["question_number"].append(question["question_number"])
        if question.query_filename != None:
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
        extra_columns = StringObjectParser.string_to_python_object(row.extra_columns)

        table_items = []
        for table_name in correct_tables.union(extra_tables):
            column_items = []
            for table_column in correct_columns.union(extra_columns):
                column_table = table_column.split(".")[0]
                column_name = table_column.split(".")[1]
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
        schema_subset = Schema(database=row.database, tables=table_items)
        subsets_questions.append((schema_subset, question))
    return results, subsets_questions



    
        

if __name__ == "__main__":
    main()