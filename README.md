
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

# Do preprocessing without subsetting

## crush4sql on Lambda1
```bash
python ./src/main.py --subsetter_name crush4sql --benchmark_name bird --filename_comments lambda1 --no_subset_generation --subsetter_preprocessing
python ./src/main.py --subsetter_name crush4sql --benchmark_name snails --filename_comments lambda1 --no_subset_generation --subsetter_preprocessing
python ./src/main.py --subsetter_name crush4sql --benchmark_name spider2 --filename_comments lambda1 --no_subset_generation --subsetter_preprocessing
```

## rslsql on Lambda1
```bash
python ./src/main.py --subsetter_name rslsql --benchmark_name bird --filename_comments lambda1 --no_subset_generation --subsetter_preprocessing
python ./src/main.py --subsetter_name rslsql --benchmark_name snails --filename_comments lambda1 --no_subset_generation --subsetter_preprocessing
python ./src/main.py --subsetter_name rslsql --benchmark_name spider2 --filename_comments lambda1 --no_subset_generation --subsetter_preprocessing
```

## TaSQL with gpt-4.1-nano
```bash
python ./src/main.py --subsetter_name tasql --benchmark_name bird --filename_comments gpt41nano --no_subset_generation --subsetter_preprocessing
python ./src/main.py --subsetter_name tasql --benchmark_name snails --filename_comments gpt41nano --no_subset_generation --subsetter_preprocessing
python ./src/main.py --subsetter_name tasql --benchmark_name spider2 --filename_comments gpt41nano --no_subset_generation --subsetter_preprocessing
```

# Do subsetting without preprocessing

## Crush4SQL on lambda1
```bash
python ./src/main.py --subsetter_name crush4sql --benchmark_name bird --filename_comments lambda1
python ./src/main.py --subsetter_name crush4sql --benchmark_name snails --filename_comments lambda1 --recover_previous
python ./src/main.py --subsetter_name crush4sql --benchmark_name spider2 --filename_comments lambda1 --recover_previous
```

## CodeS on Lambda1
```bash
python ./src/main.py --subsetter_name CodeS --benchmark_name bird --filename_comments lambda1
python ./src/main.py --subsetter_name CodeS --benchmark_name snails --filename_comments lambda1
python ./src/main.py --subsetter_name CodeS --benchmark_name spider2 --filename_comments lambda1 --recover_previous
```

## Chess with GPT4o
```bash
python ./src/main.py --subsetter_name chess --benchmark_name bird --filename_comments gpt4o --max_col_count 2500
python ./src/main.py --subsetter_name chess --benchmark_name snails --filename_comments gpt4o --max_col_count 2500
python ./src/main.py --subsetter_name chess --benchmark_name spider2 --filename_comments gpt4o --max_col_count 2500 --recover_previous
```

## DINSQL with GPT4.1
```bash
python ./src/main.py --subsetter_name DINSQL --benchmark_name bird --filename_comments gpt41 --recover_previous
python ./src/main.py --subsetter_name DINSQL --benchmark_name snails --filename_comments gpt41 --recover_previous
python ./src/main.py --subsetter_name DINSQL --benchmark_name spider2 --filename_comments gpt41 --recover_previous
```

## RSSQL with GPT4.1 nano
```bash
python ./src/main.py --subsetter_name rslsql --benchmark_name bird --filename_comments gpt41nano --recover_previous
python ./src/main.py --subsetter_name rslsql --benchmark_name snails --filename_comments gpt41nano --recover_previous
python ./src/main.py --subsetter_name rslsql --benchmark_name spider2 --filename_comments gpt41nano --recover_previous
```

## RSSQL with GPT4o
```bash
python ./src/main.py --subsetter_name rslsql --benchmark_name bird --filename_comments gpt4o --recover_previous
python ./src/main.py --subsetter_name rslsql --benchmark_name snails --filename_comments gpt4o --recover_previous
python ./src/main.py --subsetter_name rslsql --benchmark_name spider2 --filename_comments gpt4o --recover_previous
```

## TASQL with GPT4.1 nano
```bash
python ./src/main.py --subsetter_name tasql --benchmark_name bird --filename_comments gpt41nano --recover_previous
python ./src/main.py --subsetter_name tasql --benchmark_name snails --filename_comments gpt41nano --recover_previous
python ./src/main.py --subsetter_name tasql --benchmark_name spider2 --filename_comments gpt41nano --recover_previous
```

## TASQL with GPT4.1
```bash
python ./src/main.py --subsetter_name tasql --benchmark_name bird --filename_comments gpt41 --recover_previous
python ./src/main.py --subsetter_name tasql --benchmark_name snails --filename_comments gpt41 --recover_previous
python ./src/main.py --subsetter_name tasql --benchmark_name spider2 --filename_comments gpt41 --recover_previous
```

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
- Switched from legacy GPT4 to GPT4.1 for cost and recency purposes. This required minor adjustments to the output parsing methods based on GPT4.1 tendencies (e.g., had to account for unrequested markdown and whitespace in the linking response).
- Isolated the subsetter agent functionality and faithfully represented its implementation in the Skalpel DinSqlSubsetter class.
- Added additional response parsing to extract the links returned by the LLM. This addition is necessary because in the DINSQL pipeline, the response is simply passed as a string to the next agent whereas we need it to be processed and evaluated in the skalpel Schema format.
- Increased max tokens to generate to accomodate reasoning over larger schemas


# RSLSQL Modifications
- Modified db_op.py to interact with benchmark classes instead of directly with sqlite databases.
- Made a change to backwards schema linking to reflect the methodology described in the paper: the authors describe this process as generating preliminary SQL using the full schema, then extracting identifiers from the SQL that match to identifiers in the schema. However, in the code the preliminary SQL is generated using the subset (in step_1_preliminary.py).

# TASQL Modifications
- Added output instructions to base prompts because reasoning models (vs. completion models) were outputing reasoning steps.
- Updated OpenAI library calls to newer version
- Added skalpel-compatible prompt generator that retrieves schema information from Schema object instead of sqlite databases
