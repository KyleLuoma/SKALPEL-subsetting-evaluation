
from BenchmarkEmbedding.BenchmarkEmbedding import BenchmarkEmbedding
from BenchmarkEmbedding.VectorSearchResults import VectorSearchResults, WordIdentifierDistance
from BenchmarkEmbedding.IdentifierAmbiguityProblemResults import (
    IdentifierAmbiguityProblemResults,
    IdentifierAmbiguityProblemItem
)
from NlSqlBenchmark.SchemaObjects import (
    SchemaTable,
    TableColumn
)
from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory
import pandas as pd
import os
from tqdm import tqdm
from util.StringObjectParser import StringObjectParser as sop
import editdistance
import multiprocessing


RESULTS_DICT_MASTER = {
        "database": [],
        "question_number": [],
        "gold_query_tables": [],
        "all_hidden_tables": [],
        "all_value_reference_problem_columns": [],
        "missing_tables": [],
        "missing_hidden_tables": [],
        "all_ambiguous_tables": [],
        "ambiguous_extra_tables": [],
        "hallucinated_extra_tables": [],
        "hallucinated_similar_missing_tables": [],
        "missing_similar_extra_tables": [],
        "gold_query_columns": [],
        "missing_columns": [],
        "missing_value_reference_problem_columns": [],
        "all_ambiguous_columns": [],
        "ambiguous_extra_columns": [],
        "hallucinated_extra_columns": [],
        "hallucinated_similar_missing_columns": [],
        "missing_similar_extra_columns": []
    }


def main():
    results_folder = "./subsetting_results"
    diagnosis_folder = f"{results_folder}/diagnosis"

    if not os.path.exists(diagnosis_folder):
        os.makedirs(diagnosis_folder)

    xlsx_files = [f for f in os.listdir(results_folder) if f.endswith(".xlsx")]

    # with multiprocessing.Pool(processes=16) as pool:
    #     pool.map(diagnose_subsets, xlsx_files)

    # diagnose_subsets(f"subsetting-rslsql-spider2-Native-gpt4o.xlsx")
    # return
    for filename in os.listdir(results_folder):
        if ".xlsx" not in filename or "vector_qdecomp_525th" not in filename or "snails" in filename:
            continue
        diagnose_subsets(filename)
            
        # if filename.endswith(".xlsx") and not filename.startswith("subsetting-diagnosis-"):
        #     diagnosis_file = f"{diagnosis_folder}/{filename.replace('subsetting-', 'subsetting-diagnosis-')}"
        #     if not os.path.exists(diagnosis_file):
        #         diagnose_subsets(filename)


def diagnose_subsets(
        results_filename: str        
):
    print(os.getcwd())
    naturalness = "Native"
    results_folder = "./subsetting_results"
    # results_filename = f"subsetting-chess-bird-{naturalness}-gpt4o.xlsx"
    diagnosis_filename = results_filename.replace('subsetting-', 'subsetting-diagnosis-')
    if os.path.exists(f"{results_folder}/diagnosis/{diagnosis_filename}"):
        diagnosis_df = pd.read_excel(f"{results_folder}/diagnosis/{diagnosis_filename}")
    else:
        diagnosis_df = pd.DataFrame()
    if ".xlsx" not in results_filename:
        return
    results_df = pd.read_excel(f"{results_folder}/{results_filename}")
    filename_params = results_filename.split("-")
    subsetter_name = filename_params[1]
    benchmark_name = filename_params[2]

    benchmark_factory = NlSqlBenchmarkFactory()
    benchmark = benchmark_factory.build_benchmark(benchmark_name=benchmark_name)

    benchmark_embedding = BenchmarkEmbedding(
        benchmark_name=benchmark_name, 
        build_database_on_init=True,
        db_host_profile="remote",
        db_host="cdas2",
        cuda_device=2
        )

    ### Benchmark encoding tasks, uncomment as-needed ###

    # benchmark_embedding.encode_benchmark(benchmark)
    # benchmark_embedding.encode_benchmark_questions(benchmark)
    # benchmark_embedding.encode_benchmark_gold_query_identifiers(benchmark=benchmark)
    # benchmark_embedding.encode_benchmark_values(benchmark=benchmark) #All values no longer needed
    # benchmark_embedding.encode_benchmark_gold_query_predicates(benchmark=benchmark)
    # return

    results_dict = {
        k: [
            sop.string_to_python_object(obj, True) if obj != "set()" else set() 
            for obj in diagnosis_df[k].to_list()
            ] 
        if k in diagnosis_df.columns else [] for k in RESULTS_DICT_MASTER.keys() 
    }
    for col in results_dict:
        for ix, val in enumerate(results_dict[col]):
            if type(val) == str and val[0] == "{":
                val = set([s.strip() for s in val.replace("{", "").replace("}", "").split(".")])
    # results_dict["hallucinated_extra_columns"] = []
    # results_dict["hallucinated_extra_tables"] = []
    # results_dict["hallucinated_similar_missing_columns"] = []
    # results_dict["hallucinated_similar_missing_tables"] = []
    # results_dict["missing_similar_extra_columns"] = []
    # results_dict["missing_similar_extra_tables"] = []

    value_reference_problem_results_dict = {
        "table_name": [],
        "column_name": [],
        "text_value": [],
        "nlq_ngram": []
    }

 
    for ix, row in enumerate(
        tqdm(results_df.itertuples(), total=results_df.shape[0], desc=f"Diagnosing {results_filename}")
        ):

        correct_tables = set(sop.string_to_python_object(row.correct_tables, use_eval=True))
        missing_tables = set(sop.string_to_python_object(row.missing_tables, use_eval=True))
        
        if "gold_query_tables" not in diagnosis_df.columns:
            results_dict["gold_query_tables"].append(
                correct_tables.union(missing_tables)
            )

        if "database" not in diagnosis_df.columns:
            results_dict["database"].append(row.database)

        if "question_number" not in diagnosis_df.columns:
            results_dict["question_number"].append(row.question_number)
        
        if "missing_tables" not in diagnosis_df.columns:
            if len(missing_tables) == 0:
                results_dict["missing_tables"].append("{}")
            else:
                results_dict["missing_tables"].append(missing_tables)

        if (
            "gold_query_columns" not in diagnosis_df.columns
            and "missing_columns" not in diagnosis_df.columns
            ):
            correct_attributes = set(sop.string_to_python_object(row.correct_columns, use_eval=True))
            missing_attributes = set(sop.string_to_python_object(row.missing_columns, use_eval=True))
            results_dict["gold_query_columns"].append(
                correct_attributes.union(missing_attributes)
            )
            if len(missing_attributes) == 0:
                results_dict["missing_columns"].append("{}")
            else:
                results_dict["missing_columns"].append(missing_attributes)

        ## Discover hidden relation problems ##
        if (
            "all_hidden_tables" not in diagnosis_df.columns 
            and "missing_hidden_tables" not in diagnosis_df.columns
            ):
            hidden_relations = benchmark_embedding.get_hidden_relations(
                database_name=row.database,
                question_number=row.question_number,
                naturalness=naturalness
            )
            if len(hidden_relations) == 0:
                results_dict["all_hidden_tables"].append("{}")
            else:
                results_dict["all_hidden_tables"].append(hidden_relations)

            missing_hidden_relations = hidden_relations.intersection(missing_tables)
            if len(missing_hidden_relations) == 0:
                results_dict["missing_hidden_tables"].append("{}")
            else:
                results_dict["missing_hidden_tables"].append(missing_hidden_relations)

        ## Discover value reference problems ##
        if (
            "all_value_reference_problem_columns" not in diagnosis_df.columns 
            and "missing_value_reference_problem_columns" not in diagnosis_df.columns
            ):
            value_reference_problems = benchmark_embedding.get_value_reference_problem_results(
                database_name=row.database,
                question_number=row.question_number,
                naturalness=naturalness
            )
            question_vrp_items_dict = value_reference_problems.to_dict()
            for column in question_vrp_items_dict:
                value_reference_problem_results_dict[column] += question_vrp_items_dict[column]

            unmatched_attributes = {}
            all_value_reference_problem_columns = {}
            missing_columns_upper = [c.upper() for c in missing_attributes]
            for col in value_reference_problems.problem_columns:
                if f"{col.table_name}.{col.column_name}" not in all_value_reference_problem_columns.keys():
                    all_value_reference_problem_columns[f"{col.table_name}.{col.column_name}"] = [(col.nlq_ngram, col.db_text_value)]
                else:
                    all_value_reference_problem_columns[f"{col.table_name}.{col.column_name}"].append((col.nlq_ngram, col.db_text_value))
                if f"{col.table_name.upper()}.{col.column_name.upper()}" not in missing_columns_upper:
                    continue

                if f"{col.table_name}.{col.column_name}" not in unmatched_attributes.keys():
                    unmatched_attributes[f"{col.table_name}.{col.column_name}"] = [(col.nlq_ngram, col.db_text_value)]
                else:
                    unmatched_attributes[f"{col.table_name}.{col.column_name}"].append((col.nlq_ngram, col.db_text_value))

            if len(all_value_reference_problem_columns) > 0:
                results_dict["all_value_reference_problem_columns"].append(all_value_reference_problem_columns)
            else:
                results_dict["all_value_reference_problem_columns"].append("{}")

            if len(unmatched_attributes) > 0:
                results_dict["missing_value_reference_problem_columns"].append(unmatched_attributes)
            else:
                results_dict["missing_value_reference_problem_columns"].append("{}")


        ## Discover word NL ~ Identifier ambiguity problems ##
        if (
            "all_ambiguous_columns" not in diagnosis_df.columns 
            and "ambiguous_extra_columns" not in diagnosis_df.columns
            and "all_ambiguous_tables" not in diagnosis_df.columns
            and "ambiguous_extra_tables" not in diagnosis_df.columns
            ):
            ambiguous_items = benchmark_embedding.get_identifier_ambiguity_problem_results(
                database_name=row.database,
                question_number=row.question_number,
                naturalness=naturalness
            )
            
            extra_tables = set(sop.string_to_python_object(row.extra_tables if row.extra_tables != "set()" else "{}", use_eval=True))
            extra_columns = set(sop.string_to_python_object(row.extra_columns if row.extra_columns != "set()" else "{}", use_eval=True))

            all_ambiguous_columns = {}
            matching_ambiguous_columns = {}
            all_ambiguous_tables = {}
            matching_ambiguous_tables = {}

            for item in ambiguous_items.word_nl_matches:
                matching_relations = set([r.name for r in item.matching_relations])
                matching_attributes = set([a.name_as_string() for a in item.matching_attributes])
                if len(matching_attributes) > 0:
                    all_ambiguous_columns[item.word_nl] = matching_attributes
                if len(matching_relations) > 0:
                    all_ambiguous_tables[item.word_nl] = matching_relations
                if len(extra_columns.intersection(matching_attributes)) > 0:
                    matching_ambiguous_columns[item.word_nl] = extra_columns.intersection(matching_attributes)
                if len(extra_tables.intersection(matching_relations)) > 0:
                    matching_ambiguous_tables[item.word_nl] = extra_tables.intersection(matching_relations)

            results_dict["all_ambiguous_columns"].append(all_ambiguous_columns)
            results_dict["ambiguous_extra_columns"].append(matching_ambiguous_columns)

            results_dict["all_ambiguous_tables"].append(all_ambiguous_tables)
            results_dict["ambiguous_extra_tables"].append(matching_ambiguous_tables)

        
        ## Discover extra identifiers as hallucinations ##
        if (
            "hallucinated_extra_tables" not in diagnosis_df.columns 
            and "hallucinated_extra_columns" not in diagnosis_df.columns
            ):
        # if True:
            extra_tables = set(sop.string_to_python_object(row.extra_tables if row.extra_tables != "set()" else "{}", use_eval=True))
            extra_columns = sop.string_to_python_object(row.extra_columns if row.extra_columns != "set()" else "{}", use_eval=True)
            if row.database == "SBODemoUS":
                a=1
                a=1
            if type(extra_columns) == str:
                extra_columns = [s.strip().replace("'", "") for s in extra_columns.replace("{", "").replace("}", "").split(",")]
            extra_columns = set(extra_columns)
            schema = benchmark.get_active_schema(row.database)
            hallucinated_tables = set()
            for table in extra_tables:
                if not schema.table_exists(table):
                    hallucinated_tables.add(table)
            
            if len(hallucinated_tables) > 0:
                results_dict["hallucinated_extra_tables"].append(hallucinated_tables)
            else:
                results_dict["hallucinated_extra_tables"].append("{}")

            hallucinated_columns = set()
            for column in extra_columns:
                if not schema.column_exists(column_name=column.split(".")[-1]):
                    hallucinated_columns.add(column)
            if len(hallucinated_columns) > 0:
                results_dict["hallucinated_extra_columns"].append(hallucinated_columns)
            else:
                results_dict["hallucinated_extra_columns"].append("{}")


        ## Edit distance between missing and hallucinated identifiers
        if (
            "hallucinated_similar_missing_tables" not in diagnosis_df.columns
            and "hallucinated_similar_missing_columns" not in diagnosis_df.columns
        ):
        # if True:
            hallucination_match_threshold = 3
            hallucinated_columns = results_dict["hallucinated_extra_columns"][ix]
            missing_columns = set(
                sop.string_to_python_object(row.missing_columns if row.missing_columns != "set()" else "{}", use_eval=True)
                )
            column_matches = set()
            for m_c in missing_columns:
                for h_c in hallucinated_columns:
                    if len(m_c.split(".")[-1]) <= hallucination_match_threshold or len(h_c.split(".")[-1]) <= hallucination_match_threshold:
                        continue
                    m_h_dist = editdistance.eval(m_c.split(".")[-1], h_c.split(".")[-1])
                    if m_h_dist < hallucination_match_threshold:
                        column_matches.add((h_c, m_c))
            if len(column_matches) > 0:
                results_dict["hallucinated_similar_missing_columns"].append(column_matches)
            else:
                results_dict["hallucinated_similar_missing_columns"].append("{}")

            hallucinated_tables = results_dict["hallucinated_extra_tables"][ix]
            table_matches = set()
            missing_tables = set(
                sop.string_to_python_object(row.missing_tables if row.missing_tables != "set()" else "{}", use_eval=True)
            )
            for m_t in missing_tables:
                for h_t in hallucinated_tables:
                    if len(m_t) <= hallucination_match_threshold or len(h_t) <= hallucination_match_threshold:
                        continue
                    m_h_dist = editdistance.eval(m_t, h_t)
                    if m_h_dist < hallucination_match_threshold:
                        table_matches.add((h_t, m_t))
            if len(column_matches) > 0:
                results_dict["hallucinated_similar_missing_tables"].append(table_matches)
            else:
                results_dict["hallucinated_similar_missing_tables"].append("{}")


        ## Missing - Extra identifier semantic similarity (effects of ambiguity)
        if (
            "missing_similar_extra_tables" not in diagnosis_df.columns
            and "missing_similar_extra_columns" not in diagnosis_df.columns
        ):
        # if True:
            sim_threshold = 0.7
            schema = benchmark.get_active_schema(row.database)
            missing_similar_extra_columns = set()
            missing_columns = set(
                sop.string_to_python_object(
                    row.missing_columns if row.missing_columns != "set()" else "{}", use_eval=True
                    )
                )
            extra_columns = set(
                sop.string_to_python_object(
                    row.extra_columns if row.extra_columns != "set()" else "{}", use_eval=True
                    )
                )
            for m_column in missing_columns:
                if m_column[-2:].lower() == "id":
                    continue
                for e_column in extra_columns:
                    if not schema.column_exists(e_column.split(".")[-1]):
                        continue
                    score =  benchmark_embedding.get_string_similarities(m_column.split(".")[-1], e_column.split(".")[-1]) 
                    if score >= sim_threshold:
                        missing_similar_extra_columns.add((m_column, e_column, score))

            missing_similar_extra_tables = set()
            missing_tables = set(
                sop.string_to_python_object(
                    row.missing_tables if row.missing_tables != "set()" else "{}", use_eval=True
                )
            )
            extra_tables = set(
                sop.string_to_python_object(
                    row.extra_tables if row.extra_tables != "set()" else "{}", use_eval=True
                )
            )
            for m_table in missing_tables:
                for e_table in extra_tables:
                    if not schema.table_exists(e_table):
                        continue
                    score = benchmark_embedding.get_string_similarities(m_table, e_table)
                    if score >= sim_threshold:
                        missing_similar_extra_tables.add((m_table, e_table, score))

            if len(missing_similar_extra_columns) > 0:
                results_dict["missing_similar_extra_columns"].append(missing_similar_extra_columns)
            else:
                results_dict["missing_similar_extra_columns"].append("{}")

            if len(missing_similar_extra_tables) > 0:
                results_dict["missing_similar_extra_tables"].append(missing_similar_extra_tables)
            else:
                results_dict["missing_similar_extra_tables"].append("{}")


    pd.DataFrame(results_dict).to_excel(
        f"{results_folder}/diagnosis/{diagnosis_filename}", 
        index=False
        )
    pd.DataFrame(value_reference_problem_results_dict).to_excel(
        f"{results_folder}/diagnosis/value_reference_problems_expanded/{results_filename.replace('subsetting-', 'value-reference-problem-diagnosis-lambdatest')}",
        index=False
    )

if __name__ == "__main__":
    main()