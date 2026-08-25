# PySpark Data Warehouse Project with AdventureWorks 🚀

Welcome to the **PySpark Data Warehouse Project with AdventureWorks** repository!

This project is a PySpark-based rebuild of my earlier [SQL Server data warehouse project](https://github.com/Sheyxii/sql-data-warehouse-adventureworks), demonstrating the same Medallion Architecture and Star Schema — but implemented with distributed data processing instead of T-SQL. It shows an end-to-end data engineering workflow: raw ingestion, transformation, and business-ready modeling, all built with PySpark and (optionally) Delta Lake.

---

## Data Architecture

The data architecture follows the **Medallion Architecture** with Bronze, Silver, and Gold layers:

- **Bronze Layer:** Raw AdventureWorks source data ingested as-is into Spark DataFrames and persisted (Parquet/Delta), preserving the original structure for traceability.
- **Silver Layer:** Cleansed, standardized, and conformed data — deduplication, type casting, null handling, and business rule validation applied using PySpark transformations.
- **Gold Layer:** A Sales Data Mart modeled as a **Star Schema** — a central `fact_sales` table surrounded by dimension tables (`dim_customers`, `dim_products`, `dim_territory`, `dim_sales_persons`, `dim_dates`) — ready for analytics and reporting.

---

## Project Overview

This project involves:

- **Data Architecture:** Designing a modern data warehouse using Medallion Architecture (Bronze, Silver, Gold), implemented as PySpark jobs instead of stored procedures.
- **ETL Pipelines:** Extracting, transforming, and loading AdventureWorks data using PySpark DataFrame APIs (and Spark SQL where useful).
- **Data Modeling:** Building fact and dimension tables optimized for analytical queries, written out as Parquet/Delta tables.
- **Analytics & Reporting:** Producing Gold-layer tables ready to plug into Power BI, Databricks SQL, or notebook-based analysis.

---

## Why PySpark (vs. the SQL Server version)

| Aspect | SQL Server version | PySpark version |
|---|---|---|
| Processing engine | T-SQL, stored procedures | PySpark DataFrame API / Spark SQL |
| Scalability | Single-server | Distributed, horizontally scalable |
| Storage format | SQL Server tables | Parquet / Delta Lake |
| Orchestration | Manual script execution | PySpark jobs (extensible to Airflow/Databricks Jobs) |
| Data quality checks | T-SQL validation scripts | PySpark-based validation functions |

---

## Important Links & Tools

Everything is free / open-source!

- **Dataset:** [AdventureWorks Sample Database](https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure) — Official Microsoft sample dataset (exported to CSV/Parquet for Spark ingestion).
- **PySpark:** Local Spark session (via `pyspark` package) for development.
- **Delta Lake (optional):** For ACID transactions, schema enforcement, and time travel on top of Parquet.
- **Jupyter / Databricks Community Edition:** For interactive development and testing.
- **Git Repository:** For version control and portfolio presentation.
- **DrawIO:** For architecture and data flow diagrams.

---

## Project Requirements

### Building the Data Warehouse (Data Engineering)

**Objective**
Develop a modern data warehouse using PySpark to consolidate AdventureWorks sales data, enabling analytical reporting and informed decision-making — while practicing distributed data processing patterns.

**Specifications**
- **Data Source:** AdventureWorks dataset (Sales, Customer, Product, and Territory domains), loaded as CSV/Parquet files.
- **Data Quality:** Cleanse and resolve data quality issues (nulls, duplicates, type mismatches) prior to modeling.
- **Integration:** Consolidate all relevant source tables into a single, analytics-friendly star schema.
- **Scope:** Focus on the latest snapshot of the dataset; historization/SCD is a stretch goal, not required for v1.
- **Documentation:** Clear documentation of the data model, transformations, and naming conventions.

### Analytics & Reporting (Data Analysis)

**Objective**
Develop PySpark/Spark SQL-based analytics on top of the Gold layer to provide insights into:
- Customer Behavior
- Product Performance
- Sales Trends

These insights empower stakeholders with key business metrics for strategic decision-making.

---

## Repository Structure

```
pyspark-data-warehouse-adventureworks/
│
├── datasets/                        # Raw AdventureWorks source data (CSV exports)
│
├── docs/                             # Project documentation and architecture details
│   ├── notes.md                      # Implementation plan / working notes
│   ├── data_architecture.png         # Diagram of the Bronze/Silver/Gold architecture
│   ├── data_catalog.md               # Catalog of datasets, field descriptions, and metadata
│   ├── data_flow.png                 # Data flow diagram
│   ├── data_model.png                # Star schema diagram
│   ├── naming_conventions.md         # Naming guidelines for tables, columns, and files
│
├── src/                               # PySpark source code
│   ├── config/
│   │   └── spark_session.py          # Spark session builder / config
│   ├── bronze/
│   │   └── load_bronze.py            # Ingest raw source files into the Bronze layer
│   ├── silver/
│   │   └── transform_silver.py       # Cleansing, standardization, conforming logic
│   ├── gold/
│   │   ├── build_dimensions.py       # Build dim_customers, dim_products, dim_territory, etc.
│   │   └── build_fact_sales.py       # Build the fact_sales table
│   └── utils/
│       └── data_quality.py           # Reusable validation/data-quality functions
│
├── jobs/                              # Entry-point scripts to run each layer end-to-end
│   ├── run_bronze_job.py
│   ├── run_silver_job.py
│   └── run_gold_job.py
│
├── tests/                              # Unit/data quality tests
│   ├── test_silver_checks.py          # Validation tests for the Silver layer
│   └── test_gold_checks.py            # Validation tests for the Gold layer (star schema integrity)
│
├── notebooks/                          # Exploratory notebooks (EDA, prototyping transformations)
│   └── exploration.ipynb
│
├── requirements.txt                    # Python/PySpark dependencies
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
└── .gitignore                          # Files and directories to be ignored by Git
```

---

## Related Project

The original [SQL Server version of this project](https://github.com/Sheyxii/sql-data-warehouse-adventureworks) implements the same Medallion Architecture and Star Schema using T-SQL — useful as a side-by-side comparison of the two approaches.

---

## License

This project is licensed under the [MIT License](LICENSE).
