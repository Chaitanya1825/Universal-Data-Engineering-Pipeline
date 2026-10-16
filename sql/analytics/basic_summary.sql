-- Basic Table Summary
-- Returns total row count for the specified table
-- Parameters: :table_name (validated identifier)
SELECT
    COUNT(*) AS total_rows
FROM {table_name};
