# A set of functions to parse and profile queries using our java-based query parser analyzer

import pandas as pd
import subprocess
import json
from os.path import dirname, abspath
import os

test_query = """
SELECT a from o
;
"""

class QueryProfiler:
    """A class to parse and profile queries using our java-based query parser analyzer
    
    ...

    Attributes
    ----------
    __jar_path : str
        the path to the jar file for the query parser analyzer
    query : str
        the query to be parsed and profiled
    tree : dict
        the tree representation of the query
    stats : dict
        the statistics of the query

    Methods
    -------
    profile_query(query)
        parses and profiles the query
    get_identifiers_and_labels(query, distinct, include_brackets)
        returns a dictionary of the identifiers and labels in the query
    get_identifiers_and_labels_df(query, query_num, include_brackets)
        returns a dataframe of the identifiers and labels in the query
    
    """

    def __init__(self):
        self.bin_folder = dirname(dirname(dirname(abspath(__file__)))) + "/bin"
        self.__jar_path = self.bin_folder + "/SQLParserQueryAnalyzer_jar/SQLParserQueryAnalyzer.jar"
        self.query = None
        self.tree = None
        self.stats = None
        self.use_shell = os.name != 'nt'
        pass


    def profile_query(self, query: str, dialect: str = "sqlite") -> dict:
        stats = self.__parse_query(query, syntax=dialect)
        self.query = query
        self.tree = stats['tree']
        self.stats = stats['stats']
        return stats
    

    def get_identifiers_and_labels(
            self, 
            query: str = None, 
            distinct: bool = True, 
            include_brackets: bool = True, 
            dialect: str = "sqlite"
            ) -> dict:
        if query is None:
            query = self.query
        stats = self.profile_query(query=query, dialect=dialect)['stats']
        tables = []
        columns = []
        logical_operators = []
        functions = []
        clause_dict = {}
        for stat in stats:
            if stat[0] == 'table':
                table_name = stat[1]
                if not include_brackets:
                    table_name = table_name.replace("[","").replace("]","")
                tables.append(table_name)
            elif stat[0] == 'column':
                column_name = stat[1]
                if not include_brackets:
                    column_name = column_name.replace("[","").replace("]","")
                columns.append(column_name)
            elif stat[0] == 'logical operator':
                logical_operators.append(stat[1])
            elif stat[0] == 'function':
                functions.append(stat[1])
            else:
                if stat[0] not in clause_dict:
                    clause_dict[stat[0]] = 1
                else:
                    clause_dict[stat[0]] += 1
        if distinct:
            tables = list(set(tables))
            columns = list(set(columns))
        return {
            'tables': tables,
            'columns': columns,
            'logical_operators': logical_operators,
            'functions': functions,
            'clauses': clause_dict
        }
    

    def get_identifiers_and_labels_df(
            self, 
            query=None, 
            query_num=-1, 
            include_brackets=True
            ) -> pd.DataFrame:
        """
        Returns a dataframe containing keyword / identifier-level query components

        Parameters
        ----------
        query : str
            the query to be parsed and profiled
        query_num : int
            the query number corresponding to a list of queries (default is -1)
        include_brackets : bool
            whether or not to include brackets in the identifiers (default is True)

        Returns
        -------
        pd.DataFrame
            a dataframe containing the query number, the keyword / identifier type, and the keyword / identifier

        """
        label_dict = self.get_identifiers_and_labels(query, include_brackets=include_brackets)
        query_nums = []
        query_stat_keys = []
        query_stat_values = []
        for key in label_dict:
            if key == 'tables':
                for table in label_dict[key]:
                    query_nums.append(query_num)
                    query_stat_keys.append('table')
                    query_stat_values.append(table)
            elif key == 'columns':
                for column in label_dict[key]:
                    query_nums.append(query_num)
                    query_stat_keys.append('column')
                    query_stat_values.append(column)
            elif key == 'logical_operators':
                for logical_operator in label_dict[key]:
                    query_nums.append(query_num)
                    query_stat_keys.append('logical_operator')
                    query_stat_values.append(logical_operator)
            elif key == 'functions':
                for function in label_dict[key]:
                    query_nums.append(query_num)
                    query_stat_keys.append('function')
                    query_stat_values.append(function)
            else:
                for clause in label_dict[key]:
                    for i in range(label_dict[key][clause]):
                        query_nums.append(query_num)
                        query_stat_keys.append(key)
                        query_stat_values.append(clause)
        return pd.DataFrame(
            {
                'query_num': query_nums,
                'stat_key': query_stat_keys,
                'stat_value': query_stat_values
            }
        )
    
    def tag_query(self, query: str, syntax: str = "mssql") -> dict:
        """
        Tags a query table and column names using the java-based query parser analyzer

        Parameters
        ----------
        query : str
            the query to be tagged
        syntax : str
            the syntax of the query (default is "mssql") 
            Available syntax types: mssql

        Returns
        -------
        dict
            A dictionary containing:
                - tagged_query:   the tagged query string
                - table_aliases:  table alias list
                - column_aliases: column alias list
        """
        
        query = str(query)
        query = query.upper() 
        if syntax == "sqlite":
            query = query.replace("`", "\"")
        query = query.replace('"', '\\"')
        response = subprocess.run(
            'java -jar "{j}" --schematagger "{q}" --dialect {s}'.format(
                j = self.__jar_path,
                q = query,
                s = syntax
            ),
            capture_output=True,
            shell=self.use_shell
        )
        response = str(response)
        # print("\ntag_query DEBUG:", response)
        tagged_query = ''
        aliases = ''
        if '@BEGINTAGGEDQUERY' in response and '@ENDTAGGEDQUERY' in response:
            tagged_query = str(response).split('@BEGINTAGGEDQUERY')[1]
            tagged_query = tagged_query.split('@ENDTAGGEDQUERY')[0]
        
        if '@BEGINALIASES' in response and '@ENDALIASES' in response:
            aliases = str(response).split('@BEGINALIASES')[1]
            aliases = aliases.split('@ENDALIASES')[0]

        table_alias_list = []
        if "<TABLE_ALIASES>" in aliases:
            table_aliases = aliases.split('<TABLE_ALIASES>')[1]
            table_aliases = table_aliases.split('</TABLE_ALIASES>')[0]
            table_alias_list = table_aliases.split(',')

        column_alias_list = []
        if "<COLUMN_ALIASES>" in aliases:
            column_aliases = aliases.split('<COLUMN_ALIASES>')[1]
            column_aliases = column_aliases.split('</COLUMN_ALIASES>')[0]
            column_alias_list = column_aliases.split(',')
        for c in ['\\r', '\\n']:
            tagged_query = tagged_query.replace(c, '')
        tagged_query = tagged_query.replace("\\\'", "'")
        # print("\n\n", tagged_query)

        return {
            'tagged_query': tagged_query.strip(),
            'table_aliases': table_alias_list,
            'column_aliases': column_alias_list
        }


    def __parse_query(self, query: str, syntax: str = "mssql") -> dict:
        """
        Parses a query using the java-based query parser analyzer

        Parameters
        ----------
        query : str
            the query to be parsed
        syntax : str
            the syntax of the query (default is "mssql") 
            Available syntax types: mssql, sqlite

        Returns
        -------
        dict
            a dictionary containing a lisp-style tree representation of the query and statistics 
            about the query (e.g. tables, columns, functions, etc.)
        """
        query_to_parse = query.upper()
        query_to_parse = query_to_parse.replace('"', '\\"')
        if os.name != 'nt':
            query_to_parse = query_to_parse.replace("`", "\`")
        encoded_response = subprocess.run(
            'java -jar "{j}" --query "{q}" --dialect {s}'.format(
                j = self.__jar_path,
                q = query_to_parse,
                s = syntax,
                text=True
            ),
            capture_output=True,
            shell=self.use_shell,
            text=True
        )
        response = str(encoded_response)
        # print("__parse_query DEBUG:", response)
        response = response.replace("\\n", "")
        response = response.replace("\\r", "")
        response = response.replace("\\t", "")
        response = response.replace("\\'", "'")
        response = response.replace('""', '"')
        # print("DEBUG RESPONSE", response)
        
        try:
            tree_string = response.split('@BEGINPARSETREE')[1]
            tree_string = tree_string.split('@ENDPARSETREE')[0]

            json_string = response.split('@BEGINJSON')[1]
            json_string = json_string.split('@ENDJSON')[0]
            
            while "  " in json_string:
                json_string = json_string.replace("  ", " ")

            stat_list = []
            for stat in json.loads(json_string):
                stat_list.append(self.__single_obj_dict_to_tuple(stat))
            
            if syntax == "sqlite":
                stat_list = [(t[0], t[1].replace("`", "")) for t in stat_list]                

            return {
                'stats': stat_list,
                'tree': tree_string
            }
        except Exception as e:
            print("WARNING: Encountered exception while parsing query analyzer output:")
            print(e)
            print(query, json_string)
            print("Exception", e)
            return {
                'stats': [],
                'tree': ''
            }
        
    
    def parse_tree_pretty_print(self, tree=None) -> None:
        if tree is None:
            tree = self.tree
        tab = ''
        pretty_tree = ''
        for char in tree:
            if char == '(':
                tab += '  '
                pretty_tree += char + '\n' + tab
            elif char == ')':
                tab = tab[:-2]
                pretty_tree += char
            else:
                pretty_tree += char
        print(pretty_tree)


    def __single_obj_dict_to_tuple(self, dict_in) -> tuple:
        for key in dict_in:
            return (key, dict_in[key])
    

def tag_query_test():
    test_query = """
SELECT COUNT(DISTINCT CRASH.VEHICLE_NUMBER)
FROM NTSB.dbo.CRASH
WHERE DRUG_INVOLVEMENT = 1
AND TOWED <> 3
""".strip()
    correct_output = """SELECT COUNT ( DISTINCT <TABLE_NAME> CRASH </TABLE_NAME> . <COLUMN_NAME> VEHICLE_NUMBER </COLUMN_NAME> ) FROM NTSB . DBO . <TABLE_NAME> CRASH </TABLE_NAME> WHERE <COLUMN_NAME> DRUG_INVOLVEMENT </COLUMN_NAME> = 1 AND <COLUMN_NAME> TOWED </COLUMN_NAME> < > 3"""
    qp = QueryProfiler()
    result = qp.tag_query(test_query)
    print("TEST: QueryProfiler.tag_query():", result['tagged_query'] == correct_output)


def profile_query_test():
    test_query = """
SELECT [SPECIES CODE], [SEX], AVG ([CARAPACE LENGTH]) AS CARAPACE LENGTH, AVG ([CARAPACE WIDTH]) AS CARAPACE WIDTH, AVG ([PLASTRON LENGTH]) AS PLASTRON LENGTH, AVG ([PLASTRON WIDTH]) AS PLASTRON WIDTH, AVG ([WEIGHT]) AS WEIGHT
FROM [TBLFIELDDATATURTLEMEASUREMENTS]
GROUP BY [SPECIES CODE], [SEX]
ORDER BY [SPECIES CODE], [SEX]
"""
    correct_output = [
        ('select element', '1'), 
        ('column', '[SPECIES CODE]'), 
        ('select element', '1'), 
        ('column', '[SEX]'), 
        ('select element', '1'), 
        ('function', 'AVG'), 
        ('column', '[CARAPACE LENGTH]')
        ]
    qp = QueryProfiler()
    result = qp.profile_query(test_query.upper())
    print("TEST: QueryProfiler.profile_query():", result["stats"] == correct_output)

def print_tagged_query():
    test_query = """
SELECT NumTstTakr 
FROM satscores 
WHERE cds = ( 
    SELECT CDSCode FROM frpm ORDER BY `FRPM Count (K-12)` DESC LIMIT 1 
    )
    """
    qp = QueryProfiler()
    result = qp.tag_query(test_query, syntax="sqlite")
    print(result["tagged_query"])
    result = qp.profile_query(test_query, dialect="sqlite")
    print(result["stats"])

def all_tests():
    tag_query_test()
    profile_query_test()

if __name__ == "__main__":
    all_tests()
    print_tagged_query()