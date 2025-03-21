from NlSqlBenchmark.SchemaObjects import (
    Schema,
    TableColumn,
    SchemaTable,
    ForeignKey
)
from SchemaSubsetter.CHESS.src.database_utils.schema_generator import *
from NlSqlBenchmark.NlSqlBenchmarkFactory import NlSqlBenchmarkFactory

class SkalpelDatabaseSchemaGenerator(DatabaseSchemaGenerator):
    """
    A subclass of DatabaseSchemaGenerator for Skalpel-specific schema generation.
    All methods are inherited but raise NotImplementedError.
    """

    def __init__(
            self, 
            tentative_schema: Optional[DatabaseSchema] = None, 
            schema_with_examples: Optional[DatabaseSchema] = None,
            schema_with_descriptions: Optional[DatabaseSchema] = None, 
            db_id: Optional[str] = None, 
            db_path: Optional[str] = None,
            add_examples: bool = True
            ):
        bm_factory = NlSqlBenchmarkFactory()
        self.benchmark_name = bm_factory.lookup_benchmark_by_db_name(db_id)
        self.benchmark = bm_factory.build_benchmark(self.benchmark_name)
        self.benchmark.set_active_schema(database_name=db_id)

        self.db_id = db_id
        self.db_path = db_path
        self.add_examples = add_examples
        if self.db_id not in DatabaseSchemaGenerator.CACHED_DB_SCHEMA:
            SkalpelDatabaseSchemaGenerator._load_schema_into_cache(db_id=db_id, db_path=db_path)
        self.schema_structure = tentative_schema or DatabaseSchema()
        self.schema_with_examples = schema_with_examples or DatabaseSchema()
        self.schema_with_descriptions = schema_with_descriptions or DatabaseSchema()
        self._initialize_schema_structure()

    @staticmethod
    def _set_primary_keys(db_path: str, database_schema: DatabaseSchema) -> None:
        raise NotImplementedError

    @staticmethod
    def _set_foreign_keys(db_path: str, database_schema: DatabaseSchema) -> None:
        raise NotImplementedError

    @classmethod
    def _load_schema_into_cache(cls, db_id: str, db_path: str) -> None:
        raise NotImplementedError

    def _initialize_schema_structure(self) -> None:
        raise NotImplementedError

    def _load_table_and_column_info(self) -> None:
        raise NotImplementedError

    def _load_column_examples(self) -> None:
        raise NotImplementedError

    def _load_column_descriptions(self) -> None:
        raise NotImplementedError

    def _extract_create_ddl_commands(self) -> Dict[str, str]:
        raise NotImplementedError

    @staticmethod
    def _separate_column_definitions(column_definitions: str) -> List[str]:
        raise NotImplementedError

    def _is_connection(self, table_name: str, column_name: str) -> bool:
        raise NotImplementedError

    def _get_connections(self) -> Dict[str, List[str]]:
        raise NotImplementedError

    def get_schema_with_connections(self) -> Dict[str, List[str]]:
        raise NotImplementedError

    def _get_example_column_name_description(self, table_name: str, column_name: str, include_value_description: bool = True) -> str:
        raise NotImplementedError

    def generate_schema_string(self, include_value_description: bool = True, shuffle_cols: bool = True, shuffle_tables: bool = True) -> str:
        raise NotImplementedError

    def get_column_profiles(self, with_keys: bool = False, with_references: bool = False) -> Dict[str, Dict[str, str]]:
        raise NotImplementedError