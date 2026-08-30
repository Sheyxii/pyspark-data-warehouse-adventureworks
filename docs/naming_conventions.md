# Naming Conventions

Guidelines for naming tables, columns, files, and PySpark jobs consistently across the project.

---

## Table Naming (by layer)

| Layer | Pattern | Example |
|---|---|---|
| Bronze | `aw_<source_table_name>` (snake_case of the source table) | `aw_sales_order_header`, `aw_product` |
| Silver | `aw_<source_table_name>` (same name as bronze, cleansed in place) | `aw_customer`, `aw_sales_territory` |
| Gold — dimensions | `dim_<entity>` | `dim_customers`, `dim_products`, `dim_territory` |
| Gold — facts | `fact_<subject>` | `fact_sales` |

---

## Column Naming

- All columns in `snake_case` — no camelCase or spaces.
- Surrogate keys: `<entity>_sk` (e.g. `customer_sk`, `product_sk`) — generated in the Gold layer, used as primary/foreign keys between fact and dimension tables.
- Natural/business keys: `<entity>_id` (e.g. `customer_id`, `product_id`) — preserved from the source system for traceability, but not used as join keys in the star schema.
- Foreign keys in `fact_sales` follow the surrogate key pattern: `customer_sk`, `product_sk`, `territory_sk`, `sales_person_sk`, `date_sk`.
- Boolean columns prefixed with `is_` or `has_` (e.g. `is_active`).
- Date/time columns suffixed with `_date` or `_ts` (e.g. `order_date`, `loaded_at_ts`).

---

## File & Folder Naming

- Python modules: `snake_case.py` (e.g. `load_bronze.py`, `build_fact_sales.py`).
- One file per layer responsibility — avoid mixing bronze/silver/gold logic in a single script.
- Job entry points live in `jobs/run_<layer>_job.py` and simply call functions from `src/`.
- Notebooks: `NN_description.ipynb` (e.g. `01_eda_sales_orders.ipynb`) so they sort in the order they were used.

---

## PySpark-Specific Conventions

- DataFrame variables named after the layer + entity: `bronze_customer_df`, `silver_customer_df`, `dim_customers_df`.
- Output paths mirror the layer name: `data/bronze/...`, `data/silver/...`, `data/gold/...`.
- Reusable transformation functions are verbs: `clean_customers()`, `build_dim_products()`, `add_surrogate_key()`.

---

## Metadata Columns (added during processing)

| Column | Added at layer | Purpose |
|---|---|---|
| `_loaded_at` | Bronze | Timestamp when the record was ingested |
| `_source_file` | Bronze | Original file the record came from (traceability) |
| `_is_valid` | Silver | Flag set by data quality checks |

> Adjust or trim this list as the actual pipeline is built — this is a starting convention, not a final spec.
