-- datasets.sql
-- 
SELECT
    schema_name AS dataset,
    TRIM(option_value, '"') AS description,
FROM
    `region-{region_name}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS`
WHERE
    option_name = 'description'
ORDER BY
    dataset
