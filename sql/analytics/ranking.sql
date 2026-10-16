-- Window Function: Ranking Analysis
-- Ranks rows based on an order column, with optional partition
-- Parameters: {table_name}, {ranking_func}, {order_by_column}, {direction}, {partition_clause}
SELECT
    *,
    {ranking_func}() OVER (
        {partition_clause}
        ORDER BY {order_by_column} {direction}
    ) AS calculated_rank
FROM {table_name}
LIMIT :limit_n;
