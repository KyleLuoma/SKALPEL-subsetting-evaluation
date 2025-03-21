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

from SchemaSubsetter.CHESS.src.database_utils.schema import *

@dataclass
class SchemaToDatabaseSchemaAdapter(DatabaseSchema):

    @classmethod
    def from_skalpel_schema(cls, skalpel_schema: Schema) -> "DatabaseSchema":
        database_schema = cls()
        for table in skalpel_schema.tables:
            database_schema.tables.append({
                table.name: TableSchema(columns={
                    column.name: ColumnInfo(
                        original_column_name=column.name,
                        column_name=column.name,
                        column_description=column.description,
                        value_description=column.value_description if column.value_description else "",
                        type=column.data_type,
                        value_description=column.value_description,
                        examples=column.sample_values
                    )
                    for column in table.columns
                })
            })
    