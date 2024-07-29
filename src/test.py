import tests
from tests import NlSqlBenchmarkTest


test_functions = []

# Load all test functions from modules in the tests folder
for i in tests.__dict__:
    if "__" not in i:
        print(i)
        print(tests.__dict__[i].__dict__)
        for f in tests.__dict__[i].__dict__:
            if "_test" in f:
                print(f)
                test_functions.append(tests.__dict__[i].__dict__[f])

class style():
    RED = '\033[31m'
    GREEN = '\033[32m'
    BLUE = '\033[34m'
    RESET = '\033[0m'

print("\n\n-------- RUNNING TESTS --------")
for test in test_functions:
    print(
        "MODULE:", 
        test.__module__, 
        "TEST:",
        test.__name__, 
        "RESULT:",
        {
            True: f"{style.GREEN}PASS", 
            False: f"{style.RED}FAIL"
        }[test()],
        f"{style.RESET}"
        )
