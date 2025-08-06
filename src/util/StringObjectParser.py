import warnings


class StringObjectParser:
    """
    A utility class for parsing string representations of Python objects into actual Python objects.
    Methods:
        string_to_python_object(input_string: str, use_eval: bool = False) -> object:
        detect_object_type(input_string: str) -> str:
            Detects the type of Python object represented by the input string.
        parse_string_string(input_string: str) -> str:
            Parses a string representation of a string object.
        parse_float_string(input_string: str) -> float:
            Parses a string representation of a float object.
        parse_int_string(input_string: str) -> int:
            Parses a string representation of an integer object.
        parse_set_string(input_string: str) -> set:
            Parses a string representation of a set object.
        parse_tuple_string(input_string: str) -> tuple:
            Parses a string representation of a tuple object.
        parse_list_string(input_string: str) -> list:
            Parses a string representation of a list object.
        parse_dict_string(input_string: str) -> dict:
            Parses a string representation of a dictionary object.
        recurse_on_list(input_list: list) -> list:
            Recursively parses elements of a list.
        recurse_on_set(input_set: set) -> set:
            Recursively parses elements of a set.
        recurse_on_tuple(input_tuple: tuple) -> tuple:
            Recursively parses elements of a tuple.
        recurse_on_dict(input_dict: dict) -> dict:
            Recursively parses keys and values of a dictionary.
    """


    def string_to_python_object(
            input_string: str, 
            use_eval: bool = False
            ) -> object:
        """
        Converts a string representation of a Python object into the actual Python object.
        Args:
            input_string (str): The string representation of the Python object.
            use_eval (bool): If True, uses the eval function to convert the string to a Python object.
                                The input string must be properly encased with matching symbols (e.g., [], {}, "", '').
                                Defaults to False.
        Warning:
            Using eval can be dangerous. Make sure the input string is from a trusted source.
        Returns:
            object: The Python object represented by the input string.
        Raises:
            AssertionError: If use_eval is True and the input string is not properly encased with matching symbols
        """

        if input_string == "set()":
            return set()

        if use_eval:
            warnings.warn("Using eval can be dangerous. Make sure the input string is from a trusted source.")
            assert StringObjectParser.check_valid_container(input_string)
            return eval(input_string)

        
        object_type = StringObjectParser.detect_object_type(input_string)
        parsed_object = input_string

        if object_type == "set":
            parsed_object = StringObjectParser.parse_set_string(input_string)
            parsed_object = StringObjectParser.recurse_on_set(parsed_object)

        if object_type == "str":
            parsed_object = StringObjectParser.parse_string_string(input_string)

        if object_type == "int":
            parsed_object = StringObjectParser.parse_int_string(input_string)

        if object_type == "float":
            parsed_object = StringObjectParser.parse_float_string(input_string)

        if object_type == "tuple":
            parsed_object = StringObjectParser.parse_tuple_string(input_string)
            parsed_object = StringObjectParser.recurse_on_tuple(parsed_object)

        if object_type == "list":
            parsed_object = StringObjectParser.parse_list_string(input_string)
            parsed_object = StringObjectParser.recurse_on_list(parsed_object)

        if object_type == "dict":
            parsed_object = StringObjectParser.parse_dict_string(input_string)
            parsed_object = StringObjectParser.recurse_on_dict(parsed_object)

        if parsed_object == "set()":
            parsed_object = set()
            
        return parsed_object


    @staticmethod
    def check_valid_container(input_string: str) -> bool:
        encase_symbols = {
            "[": "]",
            "(": ")",
            "{": "}",
            '"': '"',
            "'": "'"
        }
        return (
            input_string[0] in encase_symbols.keys()
            and input_string[-1] == encase_symbols[input_string[0]]
            )


    def detect_object_type(input_string: str) -> str:
        input_string = input_string.strip()
        object_type = "object"
        if input_string[0] == "{" and input_string[-1] == "}":
            if ":" not in input_string:
                object_type =  "set"
            else:
                object_type =  "dict"
        if input_string[0] == "[" and input_string[-1] == "]":
            object_type =  "list"
        if input_string[0] == "(" and input_string[-1] == ")":
            object_type =  "tuple"
        if input_string[0] in ("'", '"') and input_string[-1] in ("'", '"'):
            object_type =  "str"
        try:
            if "." in input_string and input_string.split(".")[0].isnumeric() and input_string.split(".")[1].isnumeric():
                object_type =  "float"
        except IndexError as e:
            pass
        if input_string.isnumeric():
            object_type =  "int"
        # print(input_string, "is a", object_type)
        return object_type
        


    def parse_string_string(input_string: str) -> str:
        return input_string[1:-1]
    

    def parse_float_string(input_string: str) -> float:
        return float(input_string)
    

    def parse_int_string(input_string: str) -> int:
        return int(input_string)


    def parse_set_string(input_string: str) -> set:
        input_string = input_string[1:-1]
        return set([s.strip() for s in input_string.split(",")])
    

    def parse_tuple_string(input_string: str) -> tuple:
        input_string = input_string[1:-1]
        input_list = [s.strip() for s in input_string.split(",")]
        output_tuple = tuple(input_list)
        return output_tuple
    

    def parse_list_string(input_string: str) -> list:
        input_string = input_string[1:-1]
        input_list = [s.strip() for s in input_string.split(",")]
        return input_list
        
    
    def parse_dict_string(input_string: str) -> dict:
        input_string = input_string[1:-1]
        kv_list = [s.strip() for s in input_string.split(",")]
        return {s.split(":")[0].strip(): s.split(":")[1].strip() for s in kv_list}


    def recurse_on_list(input_list: list) -> list:
        new_list = []
        for item in input_list:
            new_list.append(StringObjectParser.string_to_python_object(item))
        return new_list
    

    def recurse_on_set(input_set: set) -> set:
        new_set = set()
        for item in input_set:
            new_set.add(StringObjectParser.string_to_python_object(item))
        return new_set
    

    def recurse_on_tuple(input_tuple: tuple) -> tuple:
        tuple_list = []
        for item in input_tuple:
            tuple_list.append(StringObjectParser.string_to_python_object(item))
        return tuple(tuple_list)
    

    def recurse_on_dict(input_dict: dict) -> dict:
        output_dict = {}
        for k in input_dict:
            new_k = StringObjectParser.string_to_python_object(k)
            new_v = StringObjectParser.string_to_python_object(input_dict[k])
            output_dict[new_k] = new_v
        return output_dict
