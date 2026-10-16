-- Numeric Column Summary Statistics
-- Computes COUNT, MIN, MAX, AVG, and SUM for a numeric column
-- Parameters: {table_name}, {metric_column} (validated identifiers)
SELECT
    COUNT({metric_column}) AS populated_count,
    MIN({metric_column}) AS min_value,
    MAX({metric_column}) AS max_value,
    ROUND(CAST(AVG({metric_column}) AS NUMERIC), 4) AS avg_value,
    ROUND(CAST(SUM({metric_column}) AS NUMERIC), 4) AS total_value
FROM {table_name};
