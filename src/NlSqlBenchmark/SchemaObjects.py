

class TableColumn:

    def __init__(
            self,
            name: str,
            data_type: str = None,
            table_name: str = None,
            ):
        self.name = name
        self.data_type = data_type
        self.table_name = table_name


    def __hash__(self):
        data_type = self.data_type
        if data_type == None:
            data_type = ""
        table_name = self.table_name
        if table_name == None:
            table_name = ""
        return hash((self.name, data_type, table_name))


    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return(
            self.name == other.name
            and self.data_type == other.data_type
            and self.table_name == other.table_name
        )
    

    def __getitem__(self, item_key):
        if item_key == "name":
            return self.name
        if item_key == "data_type":
            return self.data_type
        # For backwards compatibility with dict based approach:
        if item_key == "type": 
            return self.data_type
        if item_key == "table_name":
            return self.table_name
        raise KeyError(item_key)
    

    def __str__(self):
        name = self.name_as_string()
        data_type_string = ""
        if self.data_type != None:
            data_type_string = f", data_type={self.data_type}"
        return f"TableColumn(name={name}{data_type_string})"
    

    def name_as_string(self) -> str:
        if self.table_name != None:
            name = f"{self.table_name}.{self.name}"
        else:
            name = self.name
        return name
    

class ForeignKey:
    """
    A class to represent a foreign key in a database schema.
    Attributes:
    -----------
    columns : list
        A list of columns that make up the foreign key.
    references : tuple
        A tuple containing the table name and a list of columns that the foreign key references.
    Methods:
    --------
    __init__(self, columns: list, references: tuple):
        Initializes the ForeignKey with the specified columns and references.
    __eq__(self, other):
        Checks if this ForeignKey is equal to another ForeignKey.
    __getitem__(self, item_key):
        Allows access to columns or references using a key.
    __str__(self):
        Returns a string representation of the ForeignKey.
    """

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
    

    def __str__(self):
        return f"ForeignKey(columns={self.columns}, references={self.references})"


class SchemaTable:

    def __init__(
            self,
            name: str,
            columns: list[TableColumn] = None,
            primary_keys: list[str] = None,
            foreign_keys: list[ForeignKey] = None
            ):
        self.name = name
        if columns == None:
            columns = []
        self.columns = columns
        if primary_keys == None:
            primary_keys = []
        self.primary_keys = primary_keys
        if foreign_keys == None:
            foreign_keys = None
        self.foreign_keys = foreign_keys

    
    def __hash__(self):
        columns = self.columns
        if columns == None:
            columns = []
        primary_keys = self.primary_keys
        if primary_keys == None:
            primary_keys = []
        foreign_keys = self.foreign_keys
        if foreign_keys == None:
            foreign_keys = []
        return hash((self.name, tuple(columns), tuple(primary_keys), tuple(foreign_keys)))


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
    

    def __str__(self):
        output_strings = [f"name={self.name}"]
        if self.columns != None and len(self.columns) > 0:
            columns_str = ", ".join(str(column) for column in self.columns)
            output_strings.append(f" columns=[{columns_str}]")
        if self.primary_keys != None and len(self.primary_keys) > 0:
            primary_keys_str = ", ".join(self.primary_keys)
            output_strings.append(f" primary_keys=[{primary_keys_str}]")
        if self.foreign_keys != None and len(self.foreign_keys) != None:
            foreign_keys_str = ", ".join(str(fk) for fk in self.foreign_keys)
            output_strings.append(f" foreign_keys=[{foreign_keys_str}]")
        return (f"SchemaTable({",".join(output_strings)})")




class Schema:

    def __init__(
            self,
            database: str,
            tables: list[SchemaTable],
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
    

    def __str__(self):
        tables_str = "\n  ".join(str(table) for table in self.tables)
        return f"Schema(database={self.database}, tables=[\n  {tables_str}\n])"


