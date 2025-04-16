
# Do preprocessing without subsetting

## Example
```bash
python ./src/main.py --subsetter_name crush4sql --benchmark_name snails --filename_comments lambda1 --no_subset_generation --subsetter_preprocessing
```

## Multiple Jobs
It's possible to use tmux to run multiple processing jobs simultaneously. Just avoid jobs that might create resource conflicts if you're doing performance timing.

###
```bash
tmux new -s <session_name>

# Run the command in the tmux shell

# Detach tmux session
# Ctrl+B then D

# Reattach tmux session
tmux attach -t <session_name>
```

## crush4sql on Lambda1
```bash
python ./src/main.py --subsetter_name crush4sql --benchmark_name bird --filename_comments lambda1 --no_subset_generation --subsetter_preprocessing
python ./src/main.py --subsetter_name crush4sql --benchmark_name snails --filename_comments lambda1 --no_subset_generation --subsetter_preprocessing
python ./src/main.py --subsetter_name crush4sql --benchmark_name spider2 --filename_comments lambda1 --no_subset_generation --subsetter_preprocessing
```

# Do subsetting without preprocessing

## Crush4SQL on lambda1
```bash
python ./src/main.py --subsetter_name crush4sql --benchmark_name bird --filename_comments lambda1
python ./src/main.py --subsetter_name crush4sql --benchmark_name snails --filename_comments lambda1 --recover_previous
```

## CodeS on Lambda1
```bash
python ./src/main.py --subsetter_name CodeS --benchmark_name bird --filename_comments lambda1
python ./src/main.py --subsetter_name CodeS --benchmark_name snails --filename_comments lambda1
python ./src/main.py --subsetter_name CodeS --benchmark_name spider2 --filename_comments lambda1
```

## Chess with GPT4o
```bash
python ./src/main.py --subsetter_name chess --benchmark_name bird --filename_comments gpt4o --max_col_count 2500
python ./src/main.py --subsetter_name chess --benchmark_name snails --filename_comments gpt4o --max_col_count 2500
python ./src/main.py --subsetter_name chess --benchmark_name spider2 --filename_comments gpt4o --max_col_count 2500 --recover_previous
```

## DINSQL with GPT4.1o


# Modifications made to subsetting methods

We did our best to faithfully implement the subsetting methods. However, in some cases modifications were required based on application of new benchmarks that resulted in failure states in the original code.

## Crush4SQL Modifications:
- Modifidied ```greedy_selection.greedy_select(...)``` to track iterations in the while loop. This was necessary because when schema items were less than the budget, it would enter a halting state based on the original loop exit criteria.
- Added token usage return data from Crush4SQL functions that use an LLM.
- Added additional method argument to replace global schema data (RELATION_MAP) in the Crush4SQL demo with Skalpel benchmark schema data.
- Updated OpenAI API calls to current OpenAI library standards.
- The original GPT model (a variant of davinci) is deprecated. We replaced it with GPT4o mini.

## CHESS Modifications:
- We did not modify any core functionality. 
- We did modify the sections of CHESS code that retrieved benchmark information from files stored in the file structure. This includes creating some adapter methods and classes to provide benchmark database information in the format required by CHESS.
- schema_generator.load_schema_into_cache schema exploration queries required branching options for the Spider2 benchmark to account for variances in identifier enclosures in BigQuery and Snowflake SQL dialects.

## CodeS Modifications
- No modifications required. We import ```schema_item_filter.filter_schema()``` and pass it an adapted Skalpel benchmark schema that matches the expected CodeS format.

# DINSQL Modifications
- Updated OpenAI API calls to current OpenAI library standards.
- Switched from legacy GPT4 to GPT4o for cost purposes
- Isolated the subsetter agent functionality and faithfully represented its implementation in the Skalpel DinSqlSubsetter class.