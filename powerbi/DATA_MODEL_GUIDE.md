# Power BI Data Modeling Guide: Dimensional Modeling & Semantic Architecture

This guide explains how to structure, relate, and optimize analytical datasets discovered by the Universal Data Engineering Pipeline within Power BI Desktop's semantic model.

---

## 1. Domain-Agnostic Modeling Philosophy

Because the Universal Data Engineering Pipeline accepts arbitrary structured datasets, the data modeling architecture does not enforce a rigid business-specific star schema. Instead, it follows a metadata-driven approach:

1. **Candidate Classification**: Inspecting column data types, cardinalities, uniqueness ratios, and nullability to determine whether a table behaves as a **Fact**, a **Dimension**, or a standalone **Tabular Entity**.
2. **Confirmed Verification**: Only relationships explicitly backed by database foreign keys are designated as *confirmed*. Inferred relationships are designated as *candidate* and require human modeling confirmation in Power BI.

---

## 2. Table Archetypes

### A. Fact Tables (Candidate)
* **Definition**: Tables that capture events, transactions, measurements, or observations over time.
* **Heuristics**:
  - Contains numeric metrics (`NUMERIC_METRIC`) suitable for aggregation (`SUM`, `AVG`, `MIN`, `MAX`).
  - Contains timestamps or dates (`DATETIME_DIMENSION`).
  - Contains foreign keys or identifier attributes pointing to descriptive entities.
  - High row count relative to related dimension tables.
* **Best Practice**: Set cross-filter direction to **Single** (Dimension filters Fact). Avoid many-to-many relationships where possible.

### B. Dimension Tables (Candidate)
* **Definition**: Descriptive entities that provide analytical context, grouping, and slice-and-dice attributes (who, what, where, type, status).
* **Heuristics**:
  - High proportion of text attributes (`TEXT_ATTRIBUTE`) or categorical values (`CATEGORICAL_DIMENSION`).
  - Low cardinality relative to fact tables.
  - Contains unique primary keys or candidate natural keys.
* **Best Practice**: Hide surrogate keys from report view if not needed by end-users. Mark dimensions as 1-to-Many relationships pointing to Fact tables.

### C. Dedicated Date / Calendar Dimension
* **Requirement for Time Intelligence**: Power BI's built-in time-intelligence DAX functions (`TOTALYTD`, `SAMEPERIODLASTYEAR`, `DATEADD`) function most reliably when connected to a continuous, contiguous Date table.
* **DAX Date Table Template**:
  ```dax
  DateTable = 
  VAR MinDate = DATE(2020, 1, 1)
  VAR MaxDate = DATE(2030, 12, 31)
  RETURN
  ADDCOLUMNS(
      CALENDAR(MinDate, MaxDate),
      "Year", YEAR([Date]),
      "Month Number", MONTH([Date]),
      "Month Name", FORMAT([Date], "mmmm"),
      "Year-Month", FORMAT([Date], "yyyy-mm"),
      "Quarter", "Q" & FORMAT([Date], "q"),
      "Day of Week", FORMAT([Date], "dddd"),
      "Day Number", DAY([Date])
  )
  ```
* **Mark as Date Table**: Right-click the table in the Fields list -> **Mark as date table** -> Select the `Date` column.

---

## 3. Relationship Discovery: Candidate vs. Confirmed

| Relationship Status | Source of Truth | Reliability | Power BI Implementation |
| :--- | :--- | :--- | :--- |
| **Confirmed** | PostgreSQL `FOREIGN KEY` constraints retrieved via relational catalog inspection. | 100% Deterministic | Created automatically in the semantic model. Active, 1-to-Many, Single direction. |
| **Candidate** | Column name matching (e.g. `customer_id` present in two tables) with compatible data types. | Inferred / Heuristic | Represented in `semantic_model.json` under `candidate_relationships`. Requires user validation in Power BI Desktop Model View. |

> [!IMPORTANT]
> The pipeline never fabricates confirmed foreign-key relationships based on coincidental column names. If no explicit foreign keys exist in PostgreSQL, the model reports: `"No explicit PostgreSQL foreign-key relationships discovered."`

---

## 4. Star Schema Modeling Rules in Power BI

1. **Avoid Bidirectional Cross-Filtering**: Bidirectional filtering (`Both`) can introduce circular dependencies and unexpected calculation ambiguity. Default to **Single**.
2. **Cardinality**: Ensure the "1" side of a 1-to-many relationship has strictly unique values in the key column. If duplicates exist in the dimension key, Power BI will error or degrade into an unoptimized many-to-many relationship.
3. **Organize Measures**: Create a dedicated measure container table (e.g. `_Measures`) to house calculated DAX measures separate from physical columns.
4. **Column Visibility**:
   - Hide foreign key columns in Fact tables to force users to slice by Dimension attributes.
   - Set numeric format strings (e.g., currency, decimals, percentages) in the Model View.
