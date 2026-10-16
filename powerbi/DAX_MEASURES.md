# Power BI DAX Measures Guide

This document describes how to implement and organize Data Analysis Expressions (DAX) calculations within Power BI for tables dynamically discovered and loaded by the pipeline.

---

## 1. Fundamental Principles: Columns vs. Measures

| Concept | Storage & Compute | Context | Best Used For |
| :--- | :--- | :--- | :--- |
| **Calculated Column** | Evaluated during data refresh and stored in memory in VertiPaq. Consumes RAM. | Row Context (evaluates row by row). | Slicers, grouping categories, matrix row headers, and relationship keys. |
| **Calculated Measure** | Evaluated on-the-fly during visual query rendering. Consumes CPU on demand. | Filter Context (evaluates dynamic slices created by user interactions). | KPIs, sums, averages, ratios, time comparisons, and conditional formatting. |

> [!TIP]
> **Best Practice**: Never use raw columns directly inside visuals for numeric values (implicit measures). Always author explicit DAX measures (`Total Value = SUM(...)`) to guarantee consistent filtering, formatting, and performance across reports.

---

## 2. Reusable Template Library

The repository provides two core DAX libraries in `powerbi/dax/`:

### A. Generic Aggregation Measures (`powerbi/dax/generic_measures.dax`)
Domain-neutral aggregation templates accepting arbitrary table and column placeholders:
* `Total Rows = COUNTROWS('TableName')`
* `Total Metric = SUM('TableName'[MetricColumn])`
* `Average Metric = AVERAGE('TableName'[MetricColumn])`
* `Minimum Metric = MIN('TableName'[MetricColumn])`
* `Maximum Metric = MAX('TableName'[MetricColumn])`
* `Distinct Count = DISTINCTCOUNT('TableName'[IdentifierColumn])`
* `Safe Ratio = DIVIDE([Measure_A], [Measure_B], 0)`
* `Count Non-Blank = COUNTBLANK('TableName'[ColumnName])`

### B. Time Intelligence Measures (`powerbi/dax/time_intelligence.dax`)
Chronological trend calculations that operate over a dedicated Date dimension:
* `Period to Date (YTD)`: `TOTALYTD([Total Metric], 'DateTable'[Date])`
* `Month to Date (MTD)`: `TOTALMTD([Total Metric], 'DateTable'[Date])`
* `Previous Period (Year Ago)`: `CALCULATE([Total Metric], SAMEPERIODLASTYEAR('DateTable'[Date]))`
* `Period-over-Period Delta`: `[Total Metric] - [Previous Period Metric]`
* `Period-over-Period %`: `DIVIDE([Delta], [Previous Period Metric], 0)`
* `Rolling 30 Days`: `CALCULATE([Total Metric], DATESINPERIOD('DateTable'[Date], LASTDATE('DateTable'[Date]), -30, DAY))`

---

## 3. Best Practices for Production DAX

1. **Safe Division**: Always use `DIVIDE(Numerator, Denominator, AlternateResult)` instead of `/` to gracefully prevent divide-by-zero errors.
2. **Filter Modification via `CALCULATE`**: Use `CALCULATE` when intentionally overriding the active visual filter context:
   ```dax
   All Table Total = 
   CALCULATE(
       [Total Metric],
       ALL('TableName')
   )
   ```
3. **Variable Caching (`VAR` / `RETURN`)**: Use variables to evaluate calculations once and improve formula readability and query execution speed:
   ```dax
   Percentage of Total = 
   VAR CurrentValue = [Total Metric]
   VAR GrandTotal = CALCULATE([Total Metric], ALLSELECTED('TableName'))
   RETURN
   DIVIDE(CurrentValue, GrandTotal, 0)
   ```
4. **Dedicated Measure Table**: Maintain all calculated measures inside an empty container table called `_Measures` for clean cataloging.
