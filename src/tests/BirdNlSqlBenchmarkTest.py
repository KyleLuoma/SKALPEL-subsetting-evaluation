from NlSqlBenchmark.BirdNlSqlBenchmark import BirdNlSqlBenchmark

bird_databases = {
    "debit_card_specializing", 
    "financial", 
    "formula_1", 
    "california_schools",
    "card_games",
    "european_football_2",
    "thrombosis_prediction",
    "toxicology",
    "student_club",
    "superhero",
    "codebase_community"
    }

def iter_test():
    bird = BirdNlSqlBenchmark()
    found_databases = set()
    questions = []
    for q in bird:
        found_databases.add(q["database"])
        questions.append(1)
    return found_databases == bird_databases and len(questions) == 1534