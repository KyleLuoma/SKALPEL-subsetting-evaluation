
class StringObjectParser:

    def string_to_python_object(input_string: str) -> object:

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
