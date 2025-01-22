from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.CodeS import schema_item_filter as sif
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from NlSqlBenchmark.SchemaObjects import (
    Schema,
    SchemaTable,
    TableColumn,
    ForeignKey
)
from NlSqlBenchmark.BenchmarkQuestion import BenchmarkQuestion
from SchemaSubsetter.CodeS.schema_item_filter import SchemaItemClassifierInference


class CodeSSubsetter(SchemaSubsetter):

    name = "CodeS"

    def __init__(
            self,
            benchmark: NlSqlBenchmark
            ):
        self.benchmark = benchmark
        if self.benchmark.name == "bird" or self.benchmark.name == "snails":
            self.sic = SchemaItemClassifierInference(model_save_path="src/SchemaSubsetter/CodeS/sic_ckpts/sic_bird")
        self.filter_schema = sif.filter_schema



    def get_schema_subset(self, benchmark_question: BenchmarkQuestion) -> Schema:
        codes_compat_dataset = self.adapt_benchmark_schema(benchmark_question.schema, benchmark_question.question)
        codes_compat_dataset[0]["text"] = benchmark_question.question
        codes_filtered = self.filter_schema(
            dataset=codes_compat_dataset,
            dataset_type="eval",
            sic=self.sic
        )
        schema_subset = Schema(database=benchmark_question.schema.database, tables=[])
        for table in codes_filtered[0]["schema"]["schema_items"]:
            new_table = SchemaTable(
                name=table["table_name"],
                columns=[],
                primary_keys=[],
                foreign_keys=[]
            )
            for i, c in enumerate(table["column_names"]):
                new_table.columns.append(TableColumn(
                    name=c,
                    data_type=table["column_types"][i]
                ))
            schema_subset.tables.append(new_table)
        return schema_subset



    def adapt_benchmark_schema(self, schema: Schema, question: str) -> dict:
        schema_dict = {
            "schema": {"foreign_keys": []},
            "text": question,
            "matched_contents": {}
            }
        schema_dict["schema"]["schema_items"] = []
        for table in schema["tables"]:
            if len(table["primary_keys"]) > 0:
                pk_indicators = [1 if c["name"] in table["primary_keys"][0] else 0 for c in table["columns"]]
            else:
                pk_indicators = [0 for c in table["columns"]]
            
            schema_dict["schema"]["schema_items"].append({
                "table_name": table["name"],
                "table_comment": "",
                "column_names": [c["name"] for c in table["columns"]],
                "column_types": [c["type"] for c in table["columns"]],
                "column_comments": ["" for c in table["columns"]],
                "column_contents": [self.benchmark.get_sample_values(table["name"], c["name"], database=schema["database"]) for c in table["columns"]],
                "pk_indicators": pk_indicators,
            })
            if len(table["foreign_keys"]) > 0:
                for fk_dict in table["foreign_keys"]:
                    fk = []
                    fk.append(table)
                    fk.append(fk_dict["columns"][0])
                    ref_table = fk_dict["references"][0]
                    ref_col = fk_dict["references"][1]
                    fk += [ref_table, ref_col]
                    schema_dict["schema"]["foreign_keys"].append(fk)

        return [schema_dict]
    

