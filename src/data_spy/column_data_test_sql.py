"""Various templated queries for running column level data tests."""


template = """
WITH initial AS (
  SELECT
    '{dev_table}' AS table_name,
    '{column_name}' AS column_name,
    '{test_name}' AS test,
    '{dev_schema}' AS schema,
    {test_formula} AS value
FROM {dev_database}.{dev_schema}.{dev_table}
WHERE {dev_freshness_key} < CURRENT_DATE()

UNION ALL

SELECT
    '{prod_table}' AS table_name,
    '{column_name}' AS column_name,
    '{test_name}' AS test,
    '{prod_schema}' AS schema,
    {test_formula} AS value
FROM {prod_database}.{prod_schema}.{prod_table}
WHERE {prod_freshness_key} < CURRENT_DATE()
),

pivot AS
(
  SELECT
    *
FROM initial
PIVOT (MAX(value) FOR schema IN ('{prod_schema}','{dev_schema}')) AS p
(table_name, column_name, test, {prod_schema}, {dev_schema})
)

SELECT
    table_name,
    column_name,
    test,
    {prod_schema},
    {dev_schema},
    {diff_formula} AS diff
FROM pivot
"""


# test formulas
avg_formula = "AVG({column_name})"
sum_formula = "SUM({column_name})"
min_formula = "MIN({column_name})"
max_formula = "MAX({column_name})"
unique_formula = "COUNT(DISTINCT {column_name})"
count_nulls_formula = "SUM(CASE WHEN {column_name} IS NULL THEN 1 ELSE 0 END)"

# diff formulas
standard_diff_formula = "(({dev} - {prod}) / NULLIF({prod}, 0)) * 100"
timestamp_diff_formula = "TIMESTAMPDIFF('hour', {dev}, {prod})"
