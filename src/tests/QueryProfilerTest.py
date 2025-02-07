from SubsetEvaluator.QueryProfiler import QueryProfiler

def tag_ntsb_query_test():
    test_query = """
SELECT COUNT(DISTINCT CRASH.VEHICLE_NUMBER)
FROM NTSB.dbo.CRASH
WHERE DRUG_INVOLVEMENT = 1
AND TOWED <> 3
""".strip()
    correct_output = """SELECT COUNT ( DISTINCT <TABLE_NAME> CRASH </TABLE_NAME> . <COLUMN_NAME> VEHICLE_NUMBER </COLUMN_NAME> ) FROM NTSB . DBO . <TABLE_NAME> CRASH </TABLE_NAME> WHERE <COLUMN_NAME> DRUG_INVOLVEMENT </COLUMN_NAME> = 1 AND <COLUMN_NAME> TOWED </COLUMN_NAME> < > 3"""
    qp = QueryProfiler()
    result = qp.tag_query(test_query)
    return result['tagged_query'] == correct_output



def profile_query_test():
    test_query = """
SELECT `SPECIES CODE`, `SEX`, AVG(`CARAPACE LENGTH`) AS `CARAPACE LENGTH`, AVG(`CARAPACE WIDTH`) AS `CARAPACE WIDTH`, AVG(`PLASTRON LENGTH`) AS `PLASTRON LENGTH`, AVG(`PLASTRON WIDTH`) AS `PLASTRON WIDTH`, AVG(`WEIGHT`) AS WEIGHT
FROM `TBLFIELDDATATURTLEMEASUREMENTS`
GROUP BY `SPECIES CODE`, `SEX`
ORDER BY `SPECIES CODE`, `SEX`
"""
    correct_output = [('group by', '1'), ('column', 'SPECIES CODE'), ('column', 'SEX'), ('select element', '1'), ('column', 'SPECIES CODE'), ('select element', '1'), ('column', 'SEX'), ('select element', '1'), ('function', 'AVG'), ('column', 'CARAPACE LENGTH'), ('select element', '1'), ('function', 'AVG'), ('column', 'CARAPACE WIDTH'), ('select element', '1'), ('function', 'AVG'), ('column', 'PLASTRON LENGTH'), ('select element', '1'), ('function', 'AVG'), ('column', 'PLASTRON WIDTH'), ('select element', '1'), ('function', 'AVG'), ('column', 'WEIGHT'), ('table source item', '1'), ('table', 'TBLFIELDDATATURTLEMEASUREMENTS'), ('order by', '1'), ('column', 'SPECIES CODE'), ('column', 'SEX')]
    qp = QueryProfiler()
    result = qp.profile_query(test_query.upper())
    return result["stats"] == correct_output



def profile_query_deep_predicates_test():
    test_query = """
SELECT T1.ID, T1.ARTIST FROM CARDS AS T1 
INNER JOIN LEGALITIES AS T2 ON T1.UUID = T2.UUID 
WHERE T2.STATUS = 'LEGAL' AND T2.FORMAT = 'COMMANDER' AND (T1.POWER IS NULL OR T1.POWER = '*')
"""
    correct_output = [('where', '1'), ('logical_operator', 'AND'), ('logical_operator', 'AND'), ('predicate', '1'), ('predicatecolumn STATUS', "predicatevalue 'LEGAL'"), ('table', 'T2'), ('column', 'STATUS'), ('table', 'T2'), ('predicate', '1'), ('predicatecolumn FORMAT', "predicatevalue 'COMMANDER'"), ('table', 'T2'), ('column', 'FORMAT'), ('table', 'T2'), ('logical_operator', 'OR'), ('predicate', '1'), ('predicatecolumn POWER', 'predicatevalue NULL'), ('table', 'T1'), ('column', 'POWER'), ('table', 'T1'), ('predicate', '1'), ('predicatecolumn POWER', "predicatevalue '*'"), ('table', 'T1'), ('column', 'POWER'), ('table', 'T1'), ('select element', '1'), ('table', 'T1'), ('column', 'ID'), ('table', 'T1'), ('select element', '1'), ('table', 'T1'), ('column', 'ARTIST'), ('table', 'T1'), ('table source item', '1'), ('table', 'CARDS'), ('table source item', '1'), ('table', 'LEGALITIES'), ('join', '1'), ('predicate', '1'), ('table', 'T1'), ('column', 'UUID'), ('table', 'T1'), ('table', 'T2'), ('column', 'UUID'), ('table', 'T2')]
    qp = QueryProfiler()
    result = qp.profile_query(query=test_query, dialect="sqlite")
    print(result['stats'])
    return result["stats"] == correct_output