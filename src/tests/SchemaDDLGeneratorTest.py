from NlSqlBenchmark.SchemaDDLGenerator import SchemaDDLGenerator
from NlSqlBenchmark.SchemaObjects import *


def init_test():
    try:
        ddl = SchemaDDLGenerator()
        return True
    except:
        return False
    

def load_type_maps_test():
    ddl = SchemaDDLGenerator()
    maps = ddl._load_type_maps()
    return type(maps) == dict


def make_create_table_statement_test():
    ddl = SchemaDDLGenerator()
    sql = """CREATE TABLE test_table (
  column1 TEXT PRIMARY KEY,
  column2 TEXT,
  column3 NUMERIC,
  FOREIGN KEY (column3) REFERENCES table2 (column4)
);"""
    table = SchemaTable(
        name="test_table",
        columns=[
            TableColumn(name="column1", data_type="text"),
            TableColumn(name="column2", data_type="VARCHAR(30)"),
            TableColumn(name="column3", data_type="NUMERIC(2,4)")
        ],
        primary_keys=["column1"],
        foreign_keys=[ForeignKey(columns=["column3"], references=("table2", ["column4"]))]
    )
    return ddl._make_create_table_statement(table, "mssql", "sqlite") == sql


def define_table_creation_sequence_test():
    ddl = SchemaDDLGenerator()
    schema = Schema(
        database="test",
        tables=[
            SchemaTable(name="t3", columns=[], foreign_keys=[
                ForeignKey(columns=[], references=("t2", []))
            ]),
            SchemaTable(name="t1", columns=[], foreign_keys=None),
            SchemaTable(name="t2", columns=[], foreign_keys=[
                ForeignKey(columns=[], references=("t1", []))
            ]),
            SchemaTable(name="t4", columns=[], foreign_keys=[
                ForeignKey(columns=[], references=("t1", []))
            ]),
        ]
    )
    create_seq = ddl._define_table_creation_sequence(schema)
    return (
        create_seq == ['t1', 't2', 't4', 't3']
        or create_seq == ['t1', 't4', 't2', 't3']
        )


def make_schema_ddl_test():
    ddl = SchemaDDLGenerator()
    schema = Schema(
        database="test",
        tables=[
            SchemaTable(name="t3", columns=[TableColumn(name="c3", data_type="varchar(30)")], foreign_keys=[
                ForeignKey(columns=["c3"], references=("t2", ["c2"]))
            ]),
            SchemaTable(name="t1", columns=[TableColumn(name="c1", data_type="INT")], foreign_keys=None),
            SchemaTable(name="t2", columns=[TableColumn(name="c2", data_type="INT")], foreign_keys=[
                ForeignKey(columns=["c2"], references=("t1", ["c1"]))
            ]),
            SchemaTable(name="t4", columns=[TableColumn(name="c4", data_type="INT")], foreign_keys=[
                ForeignKey(columns=["c4"], references=("t1", ["c1"]))
            ]),
        ]
    )
    sql = ddl.make_schema_ddl(schema=schema, schema_dialect="mssql", target_dialect="sqlite")
    return sql == """CREATE TABLE `t1` (
  `c1` INTEGER
);
CREATE TABLE `t2` (
  `c2` INTEGER,
  FOREIGN KEY (`c2`) REFERENCES `t1` (`c1`)
);
CREATE TABLE `t4` (
  `c4` INTEGER,
  FOREIGN KEY (`c4`) REFERENCES `t1` (`c1`)
);
CREATE TABLE `t3` (
  `c3` TEXT,
  FOREIGN KEY (`c3`) REFERENCES `t2` (`c2`)
);"""