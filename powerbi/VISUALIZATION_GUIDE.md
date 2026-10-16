# Power BI Visualization Guide: Schema-Driven Dashboard Design

This guide outlines how to build clean, professional, and responsive dashboards in Power BI Desktop using the metadata and recommendations dynamically produced by the pipeline.

---

## 1. Visual Selection Matrix by Analytical Role

When building report pages from arbitrary datasets, match the discovered column roles to standard visual archetypes:

| Inferred Column Roles | Recommended Visual Type | Analytical Purpose | Configuration in Power BI |
| :--- | :--- | :--- | :--- |
| **Numeric Metric Alone** | **Card / KPI Card** | High-level executive snapshot. | **Fields**: Measure or numeric metric (e.g. `[Total Metric]`). |
| **Categorical Dimension + Numeric Metric** | **Clustered Bar Chart** (horizontal) or **Column Chart** (vertical) | Dimension comparison and frequency ranking. | **Y-Axis / X-Axis**: Categorical dimension.<br/>**X-Axis / Y-Axis**: Numeric measure. |
| **DateTime Dimension + Numeric Metric** | **Line Chart** or **Area Chart** | Chronological trend and volatility analysis. | **X-Axis**: Date or Timestamp.<br/>**Y-Axis**: Numeric measure. |
| **Multiple Categorical Dimensions + Numeric Metric** | **Matrix** or **Stacked Bar Chart** | Hierarchical cross-tabulation and composition. | **Rows**: Dimension 1.<br/>**Columns**: Dimension 2.<br/>**Values**: Numeric measure. |
| **Categorical Alone (Low Cardinality)** | **Slicer** (Dropdown or Tile) | Interactive canvas filtering. | **Field**: Categorical dimension. |
| **DateTime Alone** | **Date Slicer** (Between slider) | Time window filtering. | **Field**: Date column. |
| **Wide Mixed Record Data** | **Table Visual** | Granular inspection and record-level drilldown. | **Columns**: Key identifiers, categories, metrics, and timestamps. |

---

## 2. Standard 4-Zone Dashboard Canvas Layout

A professional Power BI dashboard organizes visuals into distinct functional zones:

```text
+-----------------------------------------------------------------------------------+
| ZONE 1: HEADER & SLICERS (Top 10-15% of page)                                     |
| [Dashboard Title]   [Date Slicer: Range]   [Category Slicer: Dropdown]            |
+-----------------------------------------------------------------------------------+
| ZONE 2: EXECUTIVE KPI STRIP (Next 15-20% of page)                                 |
| [ KPI Card 1 ]    [ KPI Card 2 ]    [ KPI Card 3 ]    [ KPI Card 4 ]              |
| Total Records     Total Metric      Average Metric    Active Entities             |
+-----------------------------------------------------------------------------------+
| ZONE 3: PRIMARY ANALYTICAL VISUALS (Middle 40-50% of page)                        |
| +---------------------------------------+ +-------------------------------------+ |
| | Line Chart: Chronological Trend       | | Bar Chart: Dimensional Breakdown    | |
| | (Date vs. Metric)                     | | (Category vs. Metric)               | |
| +---------------------------------------+ +-------------------------------------+ |
+-----------------------------------------------------------------------------------+
| ZONE 4: DETAILS & BREAKDOWN MATRIX (Bottom 20-30% of page)                        |
| +-------------------------------------------------------------------------------+ |
| | Matrix / Detail Table: Granular Records, Hierarchies, and Secondary Metrics   | |
| +-------------------------------------------------------------------------------+ |
+-----------------------------------------------------------------------------------+
```

---

## 3. Visual Formatting Best Practices

1. **Explicit Measures**: Always place explicit DAX measures (`[Total Value]`) in the Values well of visuals rather than dragging raw unaggregated columns.
2. **Number Formatting**: Format all numeric measures explicitly (e.g. `#,##0.00` for decimals, `0.0%` for percentages, `#,##0` for integer counts).
3. **Color Discipline**: Use a cohesive theme palette (e.g. sleek dark slate or clean corporate navy/neutral). Avoid high-saturation rainbow colors; reserve distinct accent colors for alerts, targets, or active highlights.
4. **Tooltips & Interactions**: Configure tooltip cards showing secondary context (e.g. record count, percentage of total) when hovering over trend line points or bar chart segments.
5. **Clear Visual Titles**: Replace automated visual titles with descriptive questions or statements (e.g., replace `Total Metric by Category` with `Distribution by Category Dimension`).
