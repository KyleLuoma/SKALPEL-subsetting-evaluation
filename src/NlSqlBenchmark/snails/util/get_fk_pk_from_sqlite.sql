WITH foreign_keys AS (
    SELECT 
        m.name AS fk_table,
        p.`from` AS fk_column_id,
        p.`table` AS pk_table,
        p.`to` AS pk_column,
        p.seq AS fk_ordinal,
        m.name || '_fk_' || p.seq AS fk_constraint_name
    FROM sqlite_master m
    JOIN pragma_foreign_key_list(m.name) p
    WHERE m.type = 'table'
),
primary_keys AS (
    SELECT 
        m.name AS pk_table,
        p.name AS pk_column,
        p.cid AS pk_column_id,
        m.name || '_pk' AS pk_constraint_name
    FROM sqlite_master m
    JOIN pragma_table_info(m.name) p
    WHERE m.type = 'table' AND p.pk > 0
)
SELECT 
    fk.fk_table,
    fk.fk_column_id AS fk_column,
    fk.fk_ordinal,
    fk.fk_constraint_name,
    pk.pk_constraint_name,
    fk.pk_table,
    pk.pk_column,
    pk.pk_column_id AS pk_ordinal
FROM foreign_keys fk
JOIN primary_keys pk
ON fk.pk_table = pk.pk_table AND fk.pk_column = pk.pk_column;