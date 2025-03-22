from NlSqlBenchmark.SchemaObjects import (
    Schema,
    TableColumn,
    SchemaTable,
    ForeignKey
)
from SchemaSubsetter.CHESS.src.database_utils.schema import (
    DatabaseSchema,
    ColumnInfo,
    TableSchema
)
from collections import defaultdict

from SchemaSubsetter.CHESS.src.database_utils.schema import *

@dataclass
class SchemaToDatabaseSchemaAdapter(DatabaseSchema):

    @classmethod
    def from_skalpel_schema(cls, skalpel_schema: Schema) -> "DatabaseSchema":
        database_schema = cls()
        referenced_by_lookup = defaultdict(list)
        for table in skalpel_schema.tables:
            for fk in table.foreign_keys:
                for ix, col in enumerate(fk.columns):
                    referenced_by_lookup[(
                        fk.references[0], fk.references[1][ix]
                        )].append((table.name, col))
        for table in skalpel_schema.tables:
            new_columns = {}
            for column in table.columns:
                new_column = ColumnInfo(
                    original_column_name=column.name,
                    column_name=column.name,
                    column_description=column.description,
                    value_description=column.value_description if column.value_description else "",
                    type=column.data_type,
                    examples=column.sample_values,
                    primary_key=True if column.name in table.primary_keys else False,
                    referenced_by=referenced_by_lookup[table.name, column.name]
                )
                foreign_keys = []
                for fk in table.foreign_keys:
                    if column.name not in fk.columns:
                        continue
                    col_ix = fk.columns.index(column.name)
                    foreign_keys.append((fk.references[0], fk.references[1][col_ix]))
                new_column.foreign_keys = foreign_keys
                new_columns[column.name] = new_column
            database_schema.tables[table.name] = TableSchema(
                columns=new_columns
            )
        return database_schema
    