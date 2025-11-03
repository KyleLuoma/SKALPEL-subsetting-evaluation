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

"""
This program runs all tests from the 'tests' module and displays the results.
Test functions in the test module must contain the sequence '_test' in their name.
Try to name test files to correspond to target modules, e.g. NlSqlBenchmarkTest.py for NlSqlBenchmark.py.
"""

import tests
from tests import NlSqlBenchmarkTest
from tests import BirdNlSqlBenchmarkTest
from tests import SnailsNlSqlBenchmarkTest
from tests import SpiderNlSqlBenchmarkTest
from tests import Spider2NlSqlBenchmarkTest
from tests import NlSqlBenchmarkFactoryTest
from tests import SchemaDDLGeneratorTest

from tests import QueryResultTest
from tests import QueryProfilerTest

from tests import SchemaSubsetterTest
from tests import PerfectSchemaSubsetterTest
from tests import PerfectTableSchemaSubsetterTest
from tests import DinSqlSubsetterTest
from tests import CodeSSubsetterTest
from tests import ChessSubsetterTest
from tests import Crush4SqlSubsetterTest
from tests import RslSqlSubsetterTest
from tests import TaSqlSubsetterTest
from tests import DtsSubsetterTest
from tests import SkalpelSubsetterTest

from tests import SchemaSubsetterEvaluatorTest
from tests import NlSqlEvaluatorTest
from tests import BenchmarkEmbeddingTest
from tests import StringObjectParserTest
from tests import SchemaObjectsTest
from tests import ValueReferenceProblemResultsTests
from tests import IdentifierAmbiguityProblemResultsTest
from tests import CHESS_DatabaseSchemaGeneratorTest



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
