"""
Super class for all the benchmarks we use in the SKALPEL project to evaluate schema subsetting
"""


class NlSqlBenchmark:

    def __init__(self):
        self.databases = []
        self.active_database = 0
        self.active_database_questions = []
        self.active_question_no = 0


    def __iter__(self):
        return self

    def __next__(self):
        if self.active_question_no < len(self.active_database_questions):
            self.active_question_no += 1
            return self.get_active_question()
        else:
            self.active_database += 1
        if self.active_database < len(self.databases):
            self.active_question_no = 0
            return self.get_active_question()
        else:
            raise StopIteration


    def get_active_question(self) -> dict:
        return {
            "question": "",
            "database": self.databases[self.active_database],
            "question_number": self.active_question_no
        }
    

    def __load_active_database_questions(self) -> list:
        return []


    

def iter_test():
    bm = NlSqlBenchmark()
    bm.databases = ["one", "two", "three"]
    bm.active_database_questions = ["a", "b", "c"]
    for i in bm:
        print(bm.get_active_question())

if __name__ == "__main__":
    iter_test()
