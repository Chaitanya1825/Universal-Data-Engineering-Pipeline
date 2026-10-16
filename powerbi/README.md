# Power BI Integration & Business Intelligence Layer

This directory contains the domain-agnostic **Power BI Integration & Business Intelligence Layer** for the Universal Data Engineering Pipeline.

## Overview

The pipeline ingests, cleans, validates, and models structured datasets into a PostgreSQL analytical database. This layer bridges the relational database and downstream business intelligence tools, specifically **Microsoft Power BI Desktop**, by providing:

1. **Step-by-step Connection Guidance**: Connecting Power BI Desktop to PostgreSQL using Import or DirectQuery modes without hardcoding secrets.
2. **Dynamic Semantic Model**: Machine-readable specification (`semantic_model.json`) describing discovered tables, column analytical roles, candidate measures, dimensions, and verified foreign-key relationships.
3. **Automated Visualization Recommendations**: Heuristic engine output (`dashboard_spec.json`) mapping discovered data types to optimal visual representations (KPI cards, time trends, distributions, leaderboards).
4. **Reusable DAX Formula Library**: Generic DAX measure templates (`dax/generic_measures.dax`) and time-intelligence calculations (`dax/time_intelligence.dax`).
5. **Architectural Guides**:
   - `CONNECTION_GUIDE.md`: PostgreSQL to Power BI connectivity and credential safety.
   - `DATA_MODEL_GUIDE.md`: Star schema, dimension/fact table classification, and relationship modeling.
   - `DAX_MEASURES.md`: DAX best practices, evaluation context, and formula templates.
   - `POWER_QUERY_GUIDE.md`: Power Query M best practices, keeping heavy ETL in Python.
   - `REFRESH_STRATEGY.md`: Import vs. DirectQuery vs. Incremental Refresh aligned with Stage 8.
   - `RLS_GUIDE.md`: Row-Level Security architecture and dynamic user filtering.
   - `VISUALIZATION_GUIDE.md`: Dashboard assembly patterns for arbitrary datasets.
   - `templates/README.md`: Explaining why `.pbix` binary files are assembled in Desktop.

## Directory Structure

```text
powerbi/
├── README.md                      # This overview document
├── CONNECTION_GUIDE.md            # PostgreSQL connection instructions
├── DATA_MODEL_GUIDE.md            # Dimensional modeling guidance
├── DAX_MEASURES.md                # DAX calculation guide
├── POWER_QUERY_GUIDE.md           # M transformation best practices
├── REFRESH_STRATEGY.md            # Refresh & incremental sync guide
├── RLS_GUIDE.md                   # Row-level security guide
├── VISUALIZATION_GUIDE.md         # Visual selection & dashboard design
├── semantic_model.json            # Generated semantic model metadata
├── dashboard_spec.json            # Recommended dashboard visual specs
├── dax/
│   ├── generic_measures.dax       # Domain-agnostic aggregation DAX
│   └── time_intelligence.dax      # Date & trend intelligence DAX
└── templates/
    └── README.md                  # PBIX / PBIT file guidance
```
