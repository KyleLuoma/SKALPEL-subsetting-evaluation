from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.DinSqlSubsetter import DinSqlSubsetter
from SchemaSubsetter.CodeSSubsetter import CodeSSubsetter
from SchemaSubsetter.PerfectSchemaSubsetter import PerfectSchemaSubsetter
from SubsetEvaluator.SchemaSubsetEvaluator import SchemaSubsetEvaluator
from SubsetEvaluator import QueryProfiler
import time
import pandas as pd
from tqdm import tqdm

test = False
verbose = False

def main():
    if verbose:
        v_print = print
    else:
        def dummy(*values, **kwargs) -> None:
            pass
        v_print = dummy

    bm_factory = NlSqlBenchmarkFactory()
    benchmark = bm_factory.build_benchmark("snails")
    subsetter = CodeSSubsetter(benchmark)
    # subsetter = PerfectSchemaSubsetter()
    evaluator = SchemaSubsetEvaluator()
    results = {
        "database": [],
        "question_number": [],
        "inference_time": [],
        "prompt_tokens": []
    }
    test_counter = 0
    for question in tqdm(benchmark, total=len(benchmark), desc=f"Running {subsetter.name} subsetter over {benchmark.name} benchmark."):
        test_counter += 1
        v_print(question["query"])
        v_print("--- Running subsetter ---")
        t_start = time.perf_counter()
        subset = subsetter.get_schema_subset(
            benchmark_question=question
        )
        t_end = time.perf_counter()
        v_print("Subsetting time:", str(t_end - t_start))
        results["database"].append(question["schema"]["database"])
        results["question_number"].append(question["question_number"])
        results["inference_time"].append(t_end - t_start)
        results["prompt_tokens"].append(0)
        v_print(subset)
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
        if test and test_counter > 5:
            break
    results_df = pd.DataFrame(results)
    filename_comments = "NVIDIA_RTX_2000-new_objects_test"
    results_df.to_excel(
        f"./subsetting_results/subsetting-{subsetter.name}-{benchmark.name}-{filename_comments}.xlsx",
        index=False
    )
        

if __name__ == "__main__":
    main()