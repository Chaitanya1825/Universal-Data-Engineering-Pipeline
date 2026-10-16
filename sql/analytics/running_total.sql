-- Window Function: Running Total Analysis
-- Calculates cumulative sum of a metric ordered chronologically or sequentially
-- Parameters: {table_name}, {metric_column}, {order_by_column}, {partition_clause}
SELECT
    {order_by_column},
    {metric_column},
    SUM({metric_column}) OVER (
        {partition_clause}
        ORDER BY {order_by_column} ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM {table_name}
WHERE {metric_column} IS NOT NULL
ORDER BY {order_by_column} ASC
LIMIT :limit_n;
