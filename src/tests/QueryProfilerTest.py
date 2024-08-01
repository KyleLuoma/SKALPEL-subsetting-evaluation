from QueryProfiler import QueryProfiler

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
    return result['tagged_query'] == correct_output



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
    return result["stats"] == correct_output