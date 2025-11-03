"""
Copyright 2025 Kyle Luoma

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import pandas as pd
from NlSqlBenchmark.QueryResult import QueryResult

def record_evaluation(result_df, query_id, result, message):
    result_df.loc[result_df['query_id'] == query_id, 'Semantic_Equivalence'] = result
    result_df.loc[result_df['query_id'] == query_id, 'Reason'] = message
    return result_df


def compare_gold_to_generated(
        gold_result: QueryResult, 
        generated_result: QueryResult, 
        debug: bool = False
        ):
# Fetch results from databases:
    result_dict = {
        "equivalent": False,
        "reason": ""
    }

    generated_results = pd.DataFrame(generated_result.result_set)
    gold_results = pd.DataFrame(gold_result.result_set)

    # ------ Analyze semantic correctness (i.e. same results) ------

    # Check if both results are the same size (if not, then not semantically equivalent)
    if generated_results.shape[0] != gold_results.shape[0]:
        result_dict['equivalent'] = False
        result_dict['reason'] = 'asymmetrical tuple result size'
        return result_dict

    # Check if generated columns are fewer than gold.
    # If generated is higher than gold, it's still possible that the question was answered.
    if generated_results.shape[1] < gold_results.shape[1]:
        result_dict['equivalent'] = False
        result_dict['reason'] = 'Insufficient number of columns in generated result set'
        return result_dict

    # Check if result is an empty set (if empty, then tag as undetermined)
    if generated_results.shape[0] == 0 and gold_results.shape[0] == 0:
        result_dict['equivalent'] = False
        result_dict['reason'] = 'empty result set'
        return result_dict
    
    if generated_results.shape[1] == 0:
        result_dict['equivalent'] = False
        result_dict['reason'] = 'no columns in generated result'
        return result_dict
    
    if gold_results.shape[1] == 0:
        result_dict['equivalent'] = False
        result_dict['reason'] = 'no columns in gold result'
        return result_dict

    # Compare every row of generated results to every row of gold results:
    # If any row of generated results does not match any row of gold results, then not semantically equivalent
    # If we get through all rows of generated results and they all match, then still could be semantically equivalent
    generated_gold_col_pairs = [] # List of tuples of (generated_col_name, gold_col_name) that match
    generated_sort_by_col = generated_results.columns[0]
    gold_sort_by_col = gold_results.columns[0]

    max_values = 0

    try:
        # Pair up matching columns in gold and generated results. This allows us to 
        # handle cases where the columns are in different orders, and may also
        # have different names. 
        # It's not sufficient for full evaluation because we're only sorting
        # Individual columns; but we pair up the columns here
        # and do a full dataframe comparison next.
        for generated_col_name in generated_results.columns:
            for gold_col_name in gold_results.columns:

                generated_col_temp = generated_results[generated_col_name].copy().astype(str)
                gold_col_temp = gold_results[gold_col_name].copy().astype(str)
                
                generated_col_temp.sort_values(inplace = True, ignore_index = True)
                gold_col_temp.sort_values(inplace = True, ignore_index = True)

                if generated_col_temp.equals(gold_col_temp):
                    generated_gold_col_pairs.append((generated_col_name, gold_col_name))
                    if generated_results[generated_col_name].value_counts().shape[0] > max_values:
                        max_values = generated_results[generated_col_name].value_counts().shape[0]
                        generated_sort_by_col = generated_col_name
                        gold_sort_by_col = gold_col_name

    except TypeError as e:
        result_dict['equivalent'] = False
        result_dict['reason'] = 'type error in column comparison'
        return result_dict
    except UnicodeDecodeError as e:
        result_dict['equivalent'] = False
        result_dict['reason'] = 'UnicodeDecodeError in column comparison'
        return result_dict

    match = True
    generated_results[generated_sort_by_col] = generated_results[generated_sort_by_col].astype(str)
    gold_results[gold_sort_by_col] = gold_results[gold_sort_by_col].astype(str)

    try:
        generated_results = generated_results.sort_values(by = [generated_sort_by_col])
        gold_results = gold_results.sort_values(by = [gold_sort_by_col])
    except:
        print("sorting failed")

    # Using the sorted dataframes, sorted by the most unique columns, we can no
    # compare individual records in each column. If any column does not match,
    # then the result sets are not semantically equivalent    
    col_matches = 0
    for generated_col_ix in range(0, generated_results.shape[1]):
        for gold_col_ix in range(0, gold_results.shape[1]):
            col_matched = True
            for i in range(0, generated_results.shape[0]):
                if str(generated_results.iloc[i, generated_col_ix]) != str(gold_results.iloc[i, gold_col_ix]):
                    col_matched = False
                    break
            if col_matched:
                col_matches += 1
                break
            
    if col_matches != gold_results.shape[1]:
        match = False
        notmatched_message = "full tuple compare failed"

    if not match:
        result_dict['equivalent'] = False
        result_dict['reason'] = notmatched_message
        return result_dict

    else:
        result_dict['equivalent'] = True
        result_dict['reason'] = 'full tuple compare succeeded'
        return result_dict


