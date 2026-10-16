-- Categorical Value Distribution
-- Groups by a discrete column and calculates frequencies and percentages
-- Parameters: {table_name}, {group_column} (validated identifiers), :limit_n (bound parameter)
SELECT
    COALESCE(CAST({group_column} AS VARCHAR), '__NULL__') AS category_value,
    COUNT(*) AS record_count,
    ROUND(CAST(COUNT(*) AS FLOAT) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS percentage_share
FROM {table_name}
GROUP BY {group_column}
ORDER BY record_count DESC
LIMIT :limit_n;
