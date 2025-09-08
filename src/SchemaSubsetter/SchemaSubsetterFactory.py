from SchemaSubsetter.SchemaSubsetter import SchemaSubsetter
from SchemaSubsetter.ChessSubsetter import ChessSubsetter
from SchemaSubsetter.CodeSSubsetter import CodeSSubsetter
from SchemaSubsetter.DtsSubsetter import DtsSubsetter
from SchemaSubsetter.Crush4SqlSubsetter import Crush4SqlSubsetter
from SchemaSubsetter.DinSqlSubsetter import DinSqlSubsetter
from SchemaSubsetter.RslSqlSubsetter import RslSqlSubsetter
from SchemaSubsetter.TaSqlSubsetter import TaSqlSubsetter
from SchemaSubsetter.Perfect.PerfectSchemaSubsetter import PerfectSchemaSubsetter
from SchemaSubsetter.Perfect.PerfectTableSchemaSubsetter import PerfectTableSchemaSubsetter
from SchemaSubsetter.SkalpelSubsetter import SkalpelSubsetter
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
        "dtssql",
        "perfect_subsetter",
        "perfect_table_subsetter",
        "skalpel",
        "skalpeltasql"
    ]

    subsetter_build_dict = {
        "abstract": (SchemaSubsetter, {}),
        "chess": (ChessSubsetter, {}),
        "CodeS": (CodeSSubsetter, {}),
        "crush4sql": (Crush4SqlSubsetter, {}),
        "DINSQL": (DinSqlSubsetter, {"model":"gpt-4.1"}),
        "rslsql": (RslSqlSubsetter, {}),
        "tasql": (TaSqlSubsetter, {}),
        "dtssql": (DtsSubsetter, {}),
        "perfect_subsetter": (PerfectSchemaSubsetter, {}),
        "perfect_table_subsetter": (PerfectTableSchemaSubsetter, {}),
        "skalpel": (SkalpelSubsetter, {}),
        "skalpeltasql": (SkalpelSubsetter, {"use_tasql": True, "model": "gpt-4.1-nano"})
    }
       

    def build_subsetter(self, subsetter_name: str, benchmark: NlSqlBenchmark, subsetter_init_args: dict = None) -> SchemaSubsetter:
        assert subsetter_name in SchemaSubsetterFactory.subsetter_register
        subsetter_class, init_args = SchemaSubsetterFactory.subsetter_build_dict[subsetter_name]
        if subsetter_init_args != None:
            for k in subsetter_init_args:
                init_args[k] = subsetter_init_args[k]
        if "device" in init_args.keys() and not subsetter_class.uses_gpu:
            warnings.warn("The 'device' parameter was passed but will not be used by the selected subsetter.")
            init_args.pop("device", None)
        return subsetter_class(benchmark=benchmark, **init_args)

        