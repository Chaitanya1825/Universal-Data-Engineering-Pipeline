-- Null / Data Quality Column Summary
-- Computes null count and null percentage for a specific column
-- Parameters: {table_name}, {column_name} (validated identifiers)
SELECT
    COUNT(*) AS total_rows,
    COUNT({column_name}) AS non_null_count,
    COUNT(*) - COUNT({column_name}) AS null_count,
    ROUND(CAST(COUNT(*) - COUNT({column_name}) AS FLOAT) * 100.0 / NULLIF(COUNT(*), 0), 2) AS null_percentage
FROM {table_name};
