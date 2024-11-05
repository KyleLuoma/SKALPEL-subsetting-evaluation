from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.DinSqlSubsetter import DinSqlSubsetter
from SchemaSubsetter.CodeSSubsetter import CodeSSubsetter
from SchemaSubsetEvaluator import SchemaSubsetEvaluator
import QueryProfiler
import time
import pandas as pd
from tqdm import tqdm

test = False
verbose = False

def main():
    if verbose:
        pass
    else:
        def dummy(*values, **kwargs) -> None:
            pass
        print = dummy

    bm_factory = NlSqlBenchmarkFactory()
    benchmark = bm_factory.build_benchmark("bird")
    subsetter = CodeSSubsetter(benchmark=benchmark)
    evaluator = SchemaSubsetEvaluator(benchmark=bm_factory.build_benchmark("bird"))
    results = {
        "database": [],
        "question_number": [],
        "inference_time": [],
        "prompt_tokens": []
    }
    test_counter = 0
    for question in tqdm(benchmark, total=len(benchmark), desc=f"Running {subsetter.name} subsetter over {benchmark.name} benchmark."):
        test_counter += 1
        print(question["query"])
        print("--- Running subsetter ---")
        t_start = time.perf_counter()
        subset = subsetter.get_schema_subset(
            question=question["question"],
            full_schema=question["schema"]
        )
        t_end = time.perf_counter()
        results["database"].append(question["schema"]["database"])
        results["question_number"].append(question["question_number"])
        results["inference_time"].append(t_end - t_start)
        results["prompt_tokens"].append(0)
        print(subset)
        print("--- Running evaluator ---")
        scores = evaluator.evaluate_schema_subset(
            subset,
            question["question"],
            question["schema"]
        )
        for k in scores.keys():
            if k not in results.keys():
                results[k] = []
            results[k].append(scores[k])
        print(scores)
        if test and test_counter > 5:
            break
    results_df = pd.DataFrame(results)
    filename_comments = "NVIDIA_RTX_2000"
    results_df.to_excel(
        f"./subsetting_results/subsetting-{subsetter.name}-{benchmark.name}-{filename_comments}.xlsx"
    )
        

if __name__ == "__main__":
    main()