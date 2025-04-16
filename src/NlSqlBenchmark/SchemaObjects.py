

class TableColumn:
    """
    Represents a column in a table with a name, data type, and table name.

    Attributes:
        name (str): The name of the column.
        data_type (str, optional): The data type of the column. Defaults to None.
        table_name (str, optional): The name of the table to which the column belongs. Defaults to None.
    
    Methods:
    --------
        name_as_string() -> str: Returns the column name as a string, including the table name if available.
    """


    def __init__(
            self,
            name: str,
            data_type: str = None,
            table_name: str = None,
            description: str = None,
            sample_values: list = None,
            unique_values: set = None,
            value_description: str = None
            ):
        self.name = name
        self.data_type = data_type
        self.table_name = table_name
        self.description = description
        self.value_description = value_description
        if sample_values == None:
            self.sample_values = []
        else:
            self.sample_values = sample_values
        if unique_values == None:
            self.unique_values = set()
        else:
            self.unique_values = unique_values


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
            and self.description == other.description
            and self.sample_values == other.sample_values
            and self.value_description == other.value_description
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
        if item_key == "description":
            return self.description
        if item_key == "sample_values":
            return self.sample_values
        if item_key == "value_description":
            return self.value_description
        raise KeyError(item_key)
    

    def __str__(self):
        name = self.name_as_string()
        data_type_string = ""
        if self.data_type != None:
            data_type_string = f", data_type='{self.data_type}'"
        description_string = ""
        if self.description != None:
            description_string = f", description='{self.description}'"
        value_description_string = ""
        if self.value_description != None:
            value_description_string = f", value_description='{self.value_description}'"
        return f"TableColumn(name='{name}'{data_type_string}{description_string}{value_description_string})"
    

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
    """
    A class to represent a database schema table.
    Attributes:
    -----------
    name : str
        The name of the table.
    columns : list[TableColumn], optional
        A list of columns in the table (default is None).
    primary_keys : list[str], optional
        A list of primary key column names (default is None).
    foreign_keys : list[ForeignKey], optional
        A list of foreign keys in the table (default is None).
    """


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
        output_strings = [f"name='{self.name}'"]
        if self.columns != None and len(self.columns) > 0:
            columns_str = ", ".join(str(column) for column in self.columns)
            output_strings.append(f" columns=[{columns_str}]")
        if self.primary_keys != None and len(self.primary_keys) > 0:
            primary_keys_str = ", ".join([str(key) for key in self.primary_keys])
            output_strings.append(f" primary_keys=[{primary_keys_str}]")
        if self.foreign_keys != None and len(self.foreign_keys) != None:
            foreign_keys_str = ", ".join(str(fk) for fk in self.foreign_keys)
            output_strings.append(f" foreign_keys=[{foreign_keys_str}]")
        return (f"SchemaTable({','.join(output_strings)})")
    

    def as_ddl(self, ident_enclose_l: str = "", ident_enclose_r: str = ""):
        ddl = f"CREATE TABLE {ident_enclose_l}{self.name}{ident_enclose_r} (\n"
        column_definitions = []
        for column in self.columns:
            column_def = f"  {ident_enclose_l}{column.name}{ident_enclose_r} {column.data_type}"
            if column.name in self.primary_keys:
                column_def += " PRIMARY KEY"
            column_definitions.append(column_def)
        ddl += ",\n".join(column_definitions)
        if self.foreign_keys:
            foreign_key_definitions = []
            for fk in self.foreign_keys:
                processed_fk_columns = [f"{ident_enclose_l}{c}{ident_enclose_r}" for c in fk.columns]
                fk_columns = ", ".join(processed_fk_columns)
            ref_table, ref_columns = fk.references
            processed_references = {f"{ident_enclose_l}{c}{ident_enclose_r}" for c in ref_columns}
            ref_columns_str = ", ".join(processed_references)
            foreign_key_definitions.append(
                f"  FOREIGN KEY ({fk_columns}) REFERENCES {ident_enclose_l}{ref_table}{ident_enclose_r} ({ref_columns_str})"
            )
            ddl += ",\n" + ",\n".join(foreign_key_definitions)
        ddl += "\n);" 
        return ddl
    


    def get_column_count(self) -> int:
        return len(self.columns)




class Schema:
    """
    A class to represent a database schema.
    Attributes:
    -----------
    database : str
        The name of the database.
    tables : list[SchemaTable]
        A list of SchemaTable objects representing the tables in the database.
    """


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
        tables_str = ",\n  ".join(str(table) for table in self.tables)
        return f"Schema(database='{self.database}', tables=[\n  {tables_str}\n])"
    

    def get_table_by_name(self, table_name: str):
        for t in self.tables:
            if t.name == table_name:
                return t
        raise KeyError(table_name)
    

    def get_table_count(self) -> int:
        return len(self.tables)
    

    def get_column_count(self) -> int:
        col_count = 0
        for table in self.tables:
            col_count += table.get_column_count()
        return col_count



