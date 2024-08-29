from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.CodeS import schema_item_filter as sif
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
from SchemaSubsetter.CodeS.schema_item_filter import SchemaItemClassifierInference


class CodeSSubsetter(SchemaSubsetter):

    def __init__(
            self,
            benchmark: NlSqlBenchmark
            ):
        self.benchmark = benchmark
        if self.benchmark.name == "bird":
            self.sic = SchemaItemClassifierInference(model_save_path="src/SchemaSubsetter/CodeS/sic_ckpts/sic_bird")
        self.filter_schema = sif.filter_schema


    def adapt_benchmark_schema(self, schema: dict) -> dict:
        schema_dict = {
            "schema": {},
            "text": self.benchmark.get_active_question()["question"]
            }
        schema_dict["schema"]["schema_items"] = []
        for table in schema["tables"]:
            schema_dict["schema"]["schema_items"].append({
                "table_name": table["name"],
                "table_comment": "",
                "column_names": [c["name"] for c in table["columns"]],
                "column_types": [c["type"] for c in table["columns"]],
                "column_comments": ["" for c in table["columns"]],
                "column_contents": ["" for c in table["columns"]],
                "pk_indicators": table["primary_keys"]
            })
        return [schema_dict]
    

