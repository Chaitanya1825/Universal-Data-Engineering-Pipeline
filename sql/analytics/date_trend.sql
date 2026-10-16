-- Time-Series / Date Trend Analysis
-- Aggregates records over a temporal period (day, month, year)
-- Parameters: {table_name}, {date_column}, {metric_column}, {agg_func}, {date_trunc_expr}
SELECT
    {date_trunc_expr} AS time_bucket,
    {agg_func}({metric_column}) AS metric_value,
    COUNT(*) AS record_count
FROM {table_name}
WHERE {date_column} IS NOT NULL
GROUP BY {date_trunc_expr}
ORDER BY time_bucket ASC
LIMIT :limit_n;
