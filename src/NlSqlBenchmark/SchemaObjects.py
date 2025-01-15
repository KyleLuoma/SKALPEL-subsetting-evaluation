
class Schema:

    def __init__(
            self,
            database: str,
            tables: list,
            ):
        self.database = database
        self.tables = tables


    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return (
            self.database == other.database
            and self.tables == other.tables
        )
    

    def __getitem__(self, item_key):
        if item_key == "database":
            return self.database
        if item_key == "tables":
            return self.tables    
        raise KeyError(item_key)



class SchemaTable:

    def __init__(
            self,
            name: str,
            columns: list,
            primary_keys: list,
            foreign_keys: list
            ):
        self.name = name
        self.columns = columns
        self.primary_keys = primary_keys
        self.foreign_keys = foreign_keys


    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return (
            self.name == other.name
            and self.columns == other.columns
            and self.primary_keys == other.primary_keys
            and self.foreign_keys == other.foreign_keys
        )
    

    def __getitem__(self, item_key):
        if item_key == "name":
            return self.name
        if item_key == "columns":
            return self.columns
        if item_key == "primary_keys":
            return self.primary_keys
        if item_key == "foreign_keys":
            return self.foreign_keys
        raise KeyError(item_key)



class TableColumn:

    def __init__(
            self,
            name: str,
            data_type: str
            ):
        self.name = name
        self.data_type = data_type


    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return(
            self.name == other.name
            and self.data_type == other.data_type
        )
    

    def __getitem__(self, item_key):
        if item_key == "name":
            return self.name
        if item_key == "data_type":
            return self.data_type
        # For backwards compatibility with dict based approach:
        if item_key == "type": 
            return self.data_type
        raise KeyError(item_key)



class ForeignKey:

    def __init__(
            self,
            columns: list,
            references: tuple #("table1": ["column1" ...])
            ):
        self.columns = columns
        self.references = references


    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return (
            self.columns == other.columns
            and self.references == other.references
        )
    

    def __getitem__(self, item_key):
        if item_key == "columns":
            return self.columns
        if item_key == "references":
            return self.references
        raise KeyError(item_key)