from util.StringObjectParser import StringObjectParser

def detect_object_type_list_test():
    return StringObjectParser.detect_object_type("[1,2,3]") == "list"

def detect_object_type_set_test():
    return StringObjectParser.detect_object_type("{1,2,3}") == "set"

def detect_object_type_dict_test():
    return StringObjectParser.detect_object_type("{1:2}") == "dict"

def parse_set_string_test():
    result = StringObjectParser.string_to_python_object("{1, 2, 3}") 
    return result == {1, 2, 3}

def parse_tuple_string_test():
    result = StringObjectParser.string_to_python_object("(1, 2, 3)") 
    return result == (1, 2, 3)

def parse_list_string_test():
    result = StringObjectParser.string_to_python_object("[1, 2, '4']")
    return result == [1, 2, '4']

def parse_dict_string_test():
    result = StringObjectParser.string_to_python_object("{'k': 'v', 1: 2, 3: 'four'}")
    return result == {'k': 'v', 1: 2, 3: 'four'}