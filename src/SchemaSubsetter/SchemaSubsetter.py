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
Super class for schema subsetting methods we evaluate in the SKALPEL project
"""

from NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from SchemaSubsetter.SchemaSubsetterResult import SchemaSubsetterResult


class SchemaSubsetter:

    name = "abstract"
    uses_gpu = False
    
    def __init__(self, benchmark: NlSqlBenchmark = None):
        self.benchmark = benchmark
        self.device = None
        pass


    def get_schema_subset(
            self, 
            benchmark_question: BenchmarkQuestion
            ) -> SchemaSubsetterResult:
        """
        'Abstract' method for subset generation

        Parameters
        ----------
        question: str
            The NL question to use for determining the required identifiers
        full_schema: Schema
            Full schema representation
        """
        schema = Schema(
            database="database1",
            tables=[
                SchemaTable(
                    name="table1",
                    columns=[
                        TableColumn(name="column1", data_type="int")
                    ],
                    primary_keys=[],
                    foreign_keys=[]
                )
            ]
        )
        return SchemaSubsetterResult(schema_subset=schema)
    

    def preprocess_databases(
            self, 
            exist_ok: bool = True, 
            filename_comments: str = "",
            skip_already_processed: bool = False,
            **args
            ) -> dict[str, float]:
        processing_times = {}
        for database in self.benchmark.databases:
            processing_times["database"] = 0.0
        return processing_times
