from SchemaSubsetter.CHESS.src.database_utils.schema_generator import DatabaseSchemaGenerator
from NlSqlBenchmark.snails.SnailsNlSqlBenchmark import SnailsNlSqlBenchmark

def init_test():
    try:
        schema_generator = DatabaseSchemaGenerator(db_id="PacificIslandLandbirds")
        return True
    except Exception as e:
        print(e)
        return False
    

def schema_build_test():
    schema_generator = DatabaseSchemaGenerator(db_id="PacificIslandLandbirds")
    schema = DatabaseSchemaGenerator.CACHED_DB_SCHEMA["PacificIslandLandbirds"]
    snails = SnailsNlSqlBenchmark()
    pilb_schema = snails.get_active_schema("PacificIslandLandbirds")
    return (
        len(pilb_schema.tables) == len(schema.tables)
    )