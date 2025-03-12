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
from SchemaSubsetter.DinSqlSubsetter import DinSqlSubsetter
from SchemaSubsetter.CodeSSubsetter import CodeSSubsetter
from SchemaSubsetter.Perfect.PerfectSchemaSubsetter import PerfectSchemaSubsetter
from SubsetEvaluator.SchemaSubsetEvaluator import SchemaSubsetEvaluator
from SubsetEvaluator import QueryProfiler
from util.StringObjectParser import StringObjectParser
import time
import pandas as pd
from tqdm import tqdm

test = False
verbose = False

results_filename = "./subsetting_results/subsetting-CodeS-snails-NVIDIA_RTX_2000.xlsx"
results_filename = None

if verbose:
    v_print = print
else:
    def dummy(*values, **kwargs) -> None:
        pass
    v_print = dummy

def main():
    benchmark_name = "spider2"
    bm_factory = NlSqlBenchmarkFactory()
    benchmark = bm_factory.build_benchmark(benchmark_name)
    subsetter = CodeSSubsetter(benchmark)
    # subsetter = PerfectSchemaSubsetter()

    if results_filename == None:
        results, subsets_questions = generate_subsets(subsetter=subsetter, benchmark=benchmark)
    else:
        results, subsets_questions = load_subsets_from_results(
            results_filename=results_filename, 
            benchmark=benchmark
            )
    results = evaluate_subsets(subsets=subsets_questions, results=results)
    results_df = pd.DataFrame(results)

    filename_comments = "NVIDIA_RTX_2000"
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


def generate_subsets(subsetter: SchemaSubsetter, benchmark: NlSqlBenchmark) -> tuple[dict, list]:
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
    for question in tqdm(benchmark, total=len(benchmark), desc=f"Running {subsetter.name} subsetter over {benchmark.name} benchmark."):
        test_counter += 1
        v_print("\n", question["query"])
        v_print("--- Running subsetter ---")
        t_start = time.perf_counter()
        try:
            subset = subsetter.get_schema_subset(
                benchmark_question=question
            )
        except UnboundLocalError as e:
            failures.append((question, str(e)))
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
        results["prompt_tokens"].append(0)
        v_print(subset)
        if test and test_counter > 5:
            break
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