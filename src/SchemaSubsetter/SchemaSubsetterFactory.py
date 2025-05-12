from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
# from SchemaSubsetter.ChessSubsetter import ChessSubsetter
from SchemaSubsetter.CodeSSubsetter import CodeSSubsetter
from SchemaSubsetter.Crush4SqlSubsetter import Crush4SqlSubsetter
from SchemaSubsetter.DinSqlSubsetter import DinSqlSubsetter
from SchemaSubsetter.RslSqlSubsetter import RslSqlSubsetter
from SchemaSubsetter.TaSqlSubsetter import TaSqlSubsetter
from SchemaSubsetter.Perfect.PerfectSchemaSubsetter import PerfectSchemaSubsetter
from SchemaSubsetter.Perfect.PerfectTableSchemaSubsetter import PerfectTableSchemaSubsetter
from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
import warnings

class SchemaSubsetterFactory:

    subsetter_register = [
        "abstract",
        "chess",
        "CodeS",
        "crush4sql",
        "DINSQL",
        "rslsql",
        "tasql",
        "perfect_subsetter",
        "perfect_table_subsetter"
    ]

    subsetter_build_dict = {
        "abstract": (SchemaSubsetter, {}),
        # "chess": (ChessSubsetter, {}),
        "CodeS": (CodeSSubsetter, {}),
        "crush4sql": (Crush4SqlSubsetter, {}),
        "DINSQL": (DinSqlSubsetter, {"model":"gpt-4.1"}),
        "rslsql": (RslSqlSubsetter, {}),
        "tasql": (TaSqlSubsetter, {}),
        "perfect_subsetter": (PerfectSchemaSubsetter, {}),
        "perfect_table_subsetter": (PerfectTableSchemaSubsetter, {})
    }
       

    def build_subsetter(self, subsetter_name: str, benchmark: NlSqlBenchmark, subsetter_init_args: dict = None) -> SchemaSubsetter:
        assert subsetter_name in SchemaSubsetterFactory.subsetter_register
        subsetter_class, init_args = SchemaSubsetterFactory.subsetter_build_dict[subsetter_name]
        if subsetter_init_args != None:
            init_args = subsetter_init_args
        if "device" in init_args.keys() and not subsetter_class.uses_gpu:
            warnings.warn("The 'device' parameter was passed but will not be used by the selected subsetter.")
            init_args.pop("device", None)
        return subsetter_class(benchmark=benchmark, **init_args)

        