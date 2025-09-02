from NlSqlEvaluator.NlSqlEvaluator import NlSqlEvaluator
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.bird.BirdNlSqlBenchmark import BirdNlSqlBenchmark
import pandas as pd

def init_test():
    try:
        evaluator = NlSqlEvaluator(benchmark=NlSqlBenchmark())
        return True
    except Exception as e:
        print(e)
        return False
    

def create_prompt_test():
    bm = BirdNlSqlBenchmark()
    evaluator = NlSqlEvaluator(benchmark=bm)
    prompt = evaluator._create_prompt(evaluator.benchmark.get_active_question())
    return "CREATE TABLE yearmonth" in prompt and "How many gas stations in CZE" in prompt


def _get_subsets_from_subset_df_test():
    subset_df = pd.read_excel("./src/tests/files/test_subset_df.xlsx")
    bm = BirdNlSqlBenchmark()
    evaluator = NlSqlEvaluator(benchmark=bm)
    q_list = evaluator._get_subsets_from_subset_df(subset_df)
    return len(q_list[0].schema.tables) == 2


def _nl_to_sql_test():
    bm = BirdNlSqlBenchmark()
    evaluator = NlSqlEvaluator(benchmark=bm)
    sql, token_count = evaluator._nl_to_sql(question=evaluator.benchmark.get_active_question())
    print(sql)
    return "SELECT" in sql and token_count > 0


def generate_sql_from_subset_df_test():
    subset_df = pd.read_excel("./src/tests/files/test_subset_df.xlsx")
    bm = BirdNlSqlBenchmark()
    evaluator = NlSqlEvaluator(benchmark=bm)
    try:
        df = evaluator.generate_sql_from_subset_df(subset_df=subset_df.head(3))
        return True
    except:
        return False
    return False