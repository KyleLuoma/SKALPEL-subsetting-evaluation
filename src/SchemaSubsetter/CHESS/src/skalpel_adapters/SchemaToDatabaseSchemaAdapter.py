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


class SchemaToDatabaseSchemaAdapter(DatabaseSchema):

    tables: Dict[str, TableSchema] = field(default_factory=dict)

    @classmethod
    def from_table_names(cls, table_names: List[str]) -> "DatabaseSchema":
        raise NotImplementedError
    
    @classmethod
    def from_schema_dict(cls, schema_dict: Dict[str, List[str]]) -> "DatabaseSchema":
        raise NotImplementedError
    
    @classmethod
    def from_schema_dict_with_examples(cls, schema_dict_with_info: Dict[str, Dict[str, List[str]]]) -> "DatabaseSchema":
        raise NotImplementedError
    
    @classmethod
    def from_schema_dict_with_descriptions(cls, schema_dict_with_info: Dict[str, Dict[str, Dict[str, Any]]]) -> "DatabaseSchema":
        raise NotImplementedError
    
    def get_actual_table_name(self, table_name: str) -> Optional[str]:
        return super().get_actual_table_name(table_name)
    
    def get_table_info(self, table_name: str) -> Optional[TableSchema]:
        return super().get_table_info(table_name)
    
    def get_actual_column_name(self, table_name: str, column_name: str) -> Optional[str]:
        return super().get_actual_column_name(table_name, column_name)
    
    def get_column_info(self, table_name: str, column_name: str) -> Optional[ColumnInfo]:
        return super().get_column_info(table_name, column_name)
    
    def set_columns_info(self, schema_with_info: Dict[str, Dict[str, Dict[str, Any]]]) -> None:
        raise NotImplementedError
    
    def subselect_schema(self, selected_database_schema: "DatabaseSchema") -> "DatabaseSchema":
        raise NotImplementedError

    def add_info_from_schema(self, schema: "DatabaseSchema", field_names: List[str]) -> None:
        raise NotImplementedError
    
    def to_dict(self) -> Dict[str, List[str]]:
        raise NotImplementedError
