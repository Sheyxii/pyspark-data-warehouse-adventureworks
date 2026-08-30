# Implementation Notes — PySpark Data Warehouse (AdventureWorks)

Working notes for building the star schema using PySpark. Same source tables and
business rules throughout — this file tracks which columns are used, how each
one is transformed, and what quality checks apply.

---

## Star Schema Overview

```
AdventureWorks (source)
│
├── Sales.Customer
├── Person.Person
├── Production.Product
├── Production.ProductSubcategory
├── Production.ProductCategory
├── Sales.SalesOrderHeader
├── Sales.SalesOrderDetail
├── Sales.SalesPerson
├── HumanResources.Employee
└── Sales.SalesTerritory
          │
          ▼
       Bronze  (spark.read → Parquet, as-is)
          │
          ▼
       Silver  (PySpark DataFrame transforms — cleansing, joins, standardization)
          │
          ▼
       Gold
├── dim_customer
├── dim_product
├── dim_date
├── dim_sales_person
├── dim_territory
└── fact_sales
```

| Dimension Table | Source Tables |
|---|---|
| `dim_customer` | `aw_customer` + `aw_person` + `aw_store` |
| `dim_product` | `aw_product` + `aw_product_subcategory` + `aw_product_category` |
| `dim_date` | Not in source — generated as a date spine |
| `dim_territory` | `aw_sales_territory` |
| `dim_sales_person` | `aw_sales_person` + `aw_employee` + `aw_person` |

---

## Gold Table Schemas

### `dim_date` ✅
`date_key`, `full_date`, `day`, `month`, `month_name`, `quarter`, `year`, `day_of_week`, `week_of_year`

### `dim_customer` ✅
`customer_key`, `customer_id`, `name`, `customer_type`

### `dim_product` ✅
`product_key`, `product_id`, `product_name`, `product_number`, `color`, `size`, `standard_cost`, `list_price`, `subcategory`, `category`

### `dim_sales_person` ✅
`sales_person_key`, `sales_person_id`, `full_name`, `job_title`

### `dim_territory` ✅
`territory_key`, `territory_id`, `territory_name`, `country`, `region`, `group`

### `fact_sales` ✅

| Field (aliased) | Description |
|---|---|
| `sales_key` | Surrogate key (PK) |
| `sales_order_id` | Original order ID |
| `sales_order_detail_id` | Original order detail ID |
| `date_key` | FK → `dim_date` |
| `customer_key` | FK → `dim_customer` |
| `product_key` | FK → `dim_product` |
| `sales_person_key` | FK → `dim_sales_person` |
| `territory_key` | FK → `dim_territory` |
| `order_quantity` | Quantity sold |
| `unit_price` | Price per item |
| `unit_price_discount` | Discount per item |
| `sales_amount` | Total sales amount |
| `total_product_cost` | Total cost |
| `gross_profit` | `sales_amount − total_product_cost` (calculated, not in source) |

---

## Mapping Document

Before building Silver/Gold, explicitly map **which source column is used → which
Gold column it becomes → how it's transformed in PySpark → whether it's excluded.**
No guessing column-by-column while coding.

### `bronze.aw_customer`

| Source Column | Used? | Target | PySpark Notes |
|---|:---:|---|---|
| `CustomerID` | ✅ | `dim_customer.customer_id` | Business key, no transform |
| `PersonID` | ✅ | join key | `.join(person_df, "PersonID")` to get name |
| `StoreID` | ✅ | `customer_type` | `F.when(F.col("StoreID").isNull(), "Individual").otherwise("Store")` |
| `TerritoryID` | ✅ | FK → `dim_territory` | Carried through to `fact_sales` |
| `AccountNumber` | ❌ | — | Drop column |
| `rowguid` | ❌ | — | Drop column |
| `ModifiedDate` | ❌ | — | Drop column |

### `bronze.aw_person` (for Customer)

| Source Column | Used? | Target | PySpark Notes |
|---|:---:|---|---|
| `BusinessEntityID` | ✅ | join key | Join with `Customer.PersonID` |
| `FirstName` | ✅ | `dim_customer.FirstName` | `F.trim(F.col("FirstName"))` |
| `LastName` | ✅ | `dim_customer.LastName` | `F.trim(F.col("LastName"))` |
| `FirstName` + `LastName` | ✅ | `dim_customer.FullName` | `F.concat_ws(" ", F.col("FirstName"), F.col("LastName"))` |
| `PersonType`, `NameStyle`, `Title`, `MiddleName`, `Suffix`, `EmailPromotion`, `AdditionalContactInfo`, `Demographics`, `rowguid`, `ModifiedDate` | ❌ | — | Drop columns |

### `bronze.aw_product`

| Source Column | Used? | Target | PySpark Notes |
|---|:---:|---|---|
| `ProductID` | ✅ | `dim_product.product_id` | Business key |
| `Name` | ✅ | `product_name` | `.withColumnRenamed("Name", "product_name")` |
| `ProductNumber` | ✅ | `product_number` | Keep as-is |
| `Color` | ✅ | `color` | `F.coalesce(F.col("Color"), F.lit("Unknown"))` |
| `Size` | ✅ | `size` | `F.coalesce(F.col("Size"), F.lit("Unknown"))` |
| `StandardCost` | ✅ | `standard_cost` | Keep; validate `>= 0` |
| `ListPrice` | ✅ | `list_price` | Keep; validate `>= 0` |
| `ProductSubcategoryID` | ✅ | join key | Join to `ProductSubcategory` |
| `MakeFlag`, `FinishedGoodsFlag`, `SafetyStockLevel`, `ReorderPoint`, `SizeUnitMeasureCode`, `WeightUnitMeasureCode`, `Weight`, `DaysToManufacture`, `ProductLine`, `Class`, `Style`, `ProductModelID`, `SellStartDate`, `SellEndDate`, `DiscontinuedDate`, `rowguid`, `ModifiedDate` | ❌ | — | Drop columns |

### `bronze.aw_product_subcategory`

| Source Column | Used? | Target | PySpark Notes |
|---|:---:|---|---|
| `ProductSubcategoryID` | ✅ | join key | Join from `Product` |
| `ProductCategoryID` | ✅ | join key | Join to `ProductCategory` |
| `Name` | ✅ | `subcategory` | `.withColumnRenamed("Name", "subcategory")` |
| `rowguid`, `ModifiedDate` | ❌ | — | Drop columns |

### `bronze.aw_product_category`

| Source Column | Used? | Target | PySpark Notes |
|---|:---:|---|---|
| `ProductCategoryID` | ✅ | join key | Parent category |
| `Name` | ✅ | `category` | `.withColumnRenamed("Name", "category")` |
| `rowguid`, `ModifiedDate` | ❌ | — | Drop columns |

### `bronze.aw_sales_territory`

| Source Column | Used? | Target | PySpark Notes |
|---|:---:|---|---|
| `TerritoryID` | ✅ | `territory_id` | Business key |
| `Name` | ✅ | `territory_name` | `F.trim(...)` |
| `CountryRegionCode` | ✅ | `country` | Rename |
| `Group` | ✅ | `region` / `group` | Rename based on design |
| `SalesYTD`, `SalesLastYear`, `CostYTD`, `CostLastYear`, `rowguid`, `ModifiedDate` | ❌ | — | Fact-level data, not dimension — drop |

### `bronze.aw_sales_person`

| Source Column | Used? | Target | PySpark Notes |
|---|:---:|---|---|
| `BusinessEntityID` | ✅ | `sales_person_id` | Business key |
| `TerritoryID` | ✅ | join key | Optional relationship to `dim_territory` |
| `SalesQuota`, `Bonus`, `CommissionPct`, `SalesYTD`, `SalesLastYear`, `rowguid`, `ModifiedDate` | ❌ | — | Drop columns |

### `bronze.aw_employee`

| Source Column | Used? | Target | PySpark Notes |
|---|:---:|---|---|
| `BusinessEntityID` | ✅ | join key | Join to `SalesPerson` |
| `JobTitle` | ✅ | `job_title` | `F.trim(...)` |
| `NationalIDNumber`, `LoginID`, `OrganizationNode`, `OrganizationLevel`, `BirthDate`, `MaritalStatus`, `Gender`, `HireDate`, `SalariedFlag`, `VacationHours`, `SickLeaveHours`, `CurrentFlag`, `rowguid`, `ModifiedDate` | ❌ | — | Drop columns |

### `bronze.aw_person` (for SalesPerson)

| Source Column | Used? | Target | PySpark Notes |
|---|:---:|---|---|
| `BusinessEntityID` | ✅ | join key | Join with `Employee` |
| `FirstName` + `LastName` | ✅ | `full_name` | `F.concat_ws(" ", F.col("FirstName"), F.col("LastName"))` |
| All remaining columns | ❌ | — | Drop columns |

### `dim_date` (generated — date spine)

| Column | Source | PySpark Notes |
|---|---|---|
| `date_key` | Generated | `F.date_format(F.col("full_date"), "yyyyMMdd").cast("int")` |
| `full_date` | Generated | From `F.sequence(start_date, end_date, F.expr("interval 1 day"))` |
| `day` | Derived | `F.dayofmonth("full_date")` |
| `month` | Derived | `F.month("full_date")` |
| `month_name` | Derived | `F.date_format("full_date", "MMMM")` |
| `quarter` | Derived | `F.quarter("full_date")` |
| `year` | Derived | `F.year("full_date")` |
| `day_of_week` | Derived | `F.date_format("full_date", "EEEE")` |
| `week_of_year` | Derived | `F.weekofyear("full_date")` |

> Generate the range with `spark.sql("SELECT explode(sequence(to_date('2011-01-01'), to_date('2014-12-31'), interval 1 day)) as full_date")`, then derive the rest with the functions above.

### `bronze.aw_sales_order_header`

| Source Column | Used? | Target | PySpark Notes |
|---|:---:|---|---|
| `SalesOrderID` | ✅ | `fact_sales.sales_order_id` | Business key |
| `OrderDate` | ✅ | join key → `dim_date` | Join on `date_key` |
| `CustomerID` | ✅ | join key → `dim_customer` | Lookup `customer_key` |
| `SalesPersonID` | ✅ | join key → `dim_sales_person` | Lookup `sales_person_key` |
| `TerritoryID` | ✅ | join key → `dim_territory` | Lookup `territory_key` |
| `DueDate`, `ShipDate`, `BillToAddressID`, `ShipToAddressID`, `ShipMethodID`, `CreditCardID`, `CreditCardApprovalCode`, `CurrencyRateID`, `SubTotal`, `TaxAmt`, `Freight`, `TotalDue`, `Comment`, `rowguid`, `ModifiedDate` | ❌ | — | Drop columns |

### `bronze.aw_sales_order_detail`

| Source Column | Used? | Target | PySpark Notes |
|---|:---:|---|---|
| `SalesOrderID` | ✅ | join key | Join to Header |
| `SalesOrderDetailID` | ✅ | `fact_sales.sales_order_detail_id` | Business key |
| `OrderQty` | ✅ | `order_quantity` | Keep |
| `ProductID` | ✅ | join key → `dim_product` | Lookup `product_key` |
| `UnitPrice` | ✅ | `unit_price` | Keep |
| `UnitPriceDiscount` | ✅ | `unit_price_discount` | Keep |
| `LineTotal` | ✅ | `sales_amount` | Keep |
| `CarrierTrackingNumber`, `SpecialOfferID`, `rowguid`, `ModifiedDate` | ❌ | — | Drop columns |

### Calculated field

AdventureWorks has no `Profit`/`GrossProfit` column — computed during Silver/Gold transform:

```python
fact_sales_df = fact_sales_df.withColumn(
    "gross_profit",
    F.col("sales_amount") - (F.col("order_quantity") * F.col("total_product_cost"))
)
```

---

## ETL Specification (with quality checks)

### `dim_customer`

| Source Table | Source Column | Gold Column | PySpark Transformation | Quality Check |
|---|---|---|---|---|
| `aw_customer` | `CustomerID` | `customer_id` | None | Null, Duplicate |
| `aw_customer` | `PersonID` | join | `.join(person_df, "PersonID")` | Orphan check |
| `aw_customer` | `StoreID` | `customer_type` | `F.when(col.isNull(), "Individual").otherwise("Store")` | None |
| `aw_customer` | `TerritoryID` | `fact_sales.territory_key` | Lookup to `dim_territory` | Orphan check |
| `aw_person` | `BusinessEntityID` | join | Join with `PersonID` | Duplicate check |
| `aw_person` | `FirstName` | `first_name` | `F.trim()` | Null (optional), blank check |
| `aw_person` | `LastName` | `last_name` | `F.trim()` | Null (optional), blank check |
| `aw_person` | `FirstName + LastName` | `full_name` | `F.concat_ws(" ", ...)` | None |

### `dim_product`

| Source Table | Source Column | Gold Column | PySpark Transformation | Quality Check |
|---|---|---|---|---|
| `aw_product` | `ProductID` | `product_id` | None | Null, Duplicate |
| `aw_product` | `Name` | `product_name` | `F.trim()` | Null |
| `aw_product` | `ProductNumber` | `product_number` | `F.trim()` | Null, Duplicate |
| `aw_product` | `Color` | `color` | `F.coalesce(col, F.lit("Unknown"))` | None |
| `aw_product` | `Size` | `size` | `F.coalesce(col, F.lit("Unknown"))` | None |
| `aw_product` | `StandardCost` | `standard_cost` | None | `>= 0` |
| `aw_product` | `ListPrice` | `list_price` | None | `>= 0` |
| `aw_product` | `ProductSubcategoryID` | join | Join `ProductSubcategory` | Orphan check |
| `aw_product_subcategory` | `Name` | `subcategory` | Rename | Null |
| `aw_product_subcategory` | `ProductCategoryID` | join | Join `ProductCategory` | Orphan check |
| `aw_product_category` | `Name` | `category` | Rename | Null |

### `dim_territory`

| Source Table | Source Column | Gold Column | PySpark Transformation | Quality Check |
|---|---|---|---|---|
| `aw_sales_territory` | `TerritoryID` | `territory_id` | None | Null, Duplicate |
| `aw_sales_territory` | `Name` | `territory_name` | `F.trim()` | Null |
| `aw_sales_territory` | `CountryRegionCode` | `country` | Rename | Null |
| `aw_sales_territory` | `Group` | `group` | Rename | Null |

### `dim_sales_person`

| Source Table | Source Column | Gold Column | PySpark Transformation | Quality Check |
|---|---|---|---|---|
| `aw_sales_person` | `BusinessEntityID` | `sales_person_id` | None | Null, Duplicate |
| `aw_sales_person` | `TerritoryID` | join | Lookup `dim_territory` | Orphan check |
| `aw_employee` | `BusinessEntityID` | join | Join `Employee` | Duplicate check |
| `aw_employee` | `JobTitle` | `job_title` | `F.trim()` | Null |
| `aw_person` | `BusinessEntityID` | join | Join `Person` | Duplicate check |
| `aw_person` | `FirstName` + `LastName` | `full_name` | `F.concat_ws(" ", ...)` | None |

### `dim_date`

| Source | Gold Column | PySpark Transformation | Quality Check |
|---|---|---|---|
| Generated | `date_key` | `F.date_format(full_date, "yyyyMMdd")` | Duplicate |
| Generated | `full_date` | date sequence | Null |
| Generated | `day` | `F.dayofmonth()` | None |
| Generated | `month` | `F.month()` | None |
| Generated | `month_name` | `F.date_format(full_date, "MMMM")` | None |
| Generated | `quarter` | `F.quarter()` | None |
| Generated | `year` | `F.year()` | None |
| Generated | `day_of_week` | `F.date_format(full_date, "EEEE")` | None |
| Generated | `week_of_year` | `F.weekofyear()` | None |

### `fact_sales`

| Source Table | Source Column | Gold Column |
|---|---|---|
| `aw_sales_order_header` | `SalesOrderID` | `sales_order_id` |
| `aw_sales_order_detail` | `SalesOrderDetailID` | `sales_order_detail_id` |
| `aw_sales_order_header` | `OrderDate` | `date_key` |
| `aw_sales_order_header` | `CustomerID` | `customer_key` |
| `aw_sales_order_detail` | `ProductID` | `product_key` |
| `aw_sales_order_header` | `SalesPersonID` | `sales_person_key` |
| `aw_sales_order_header` | `TerritoryID` | `territory_key` |
| `aw_sales_order_detail` | `OrderQty` | `order_quantity` |
| `aw_sales_order_detail` | `UnitPrice` | `unit_price` |
| `aw_sales_order_detail` | `UnitPriceDiscount` | `unit_price_discount` |
| `aw_sales_order_detail` | `LineTotal` | `sales_amount` |
| `aw_product` | `StandardCost` | `total_product_cost` |
| Calculated | `Profit` (see formula above) | `gross_profit` |

---

## Implementation Plan

- [ ] **Phase 1 — Setup**
  - [ ] Export AdventureWorks source tables to CSV (`datasets/`)
  - [ ] Set up `config/spark_session.py`
  - [ ] Confirm `requirements.txt` installs cleanly in a fresh venv

- [ ] **Phase 2 — Bronze Layer**
  - [ ] `bronze/load_bronze.py` — ingest all source tables as-is into Parquet
  - [ ] No transformations at this stage — raw data only, for traceability

- [ ] **Phase 3 — Silver Layer**
  - [ ] `silver/transform_silver.py` — apply the column-level mapping above (drop unused columns, cleanse, standardize)
  - [ ] Apply quality checks listed per column (null, duplicate, orphan, range checks)

- [ ] **Phase 4 — Gold Layer**
  - [ ] `gold/build_dimensions.py` — build all 5 dimension tables per the ETL spec above
  - [ ] `gold/build_fact_sales.py` — build `fact_sales`, including the calculated `gross_profit` field
  - [ ] Assign surrogate keys (`*_key`) to all dimension tables

- [ ] **Phase 5 — Validation & Docs**
  - [ ] `tests/test_silver_checks.py` — row counts, null checks, referential integrity
  - [ ] `tests/test_gold_checks.py` — star schema integrity (no orphan FKs)
  - [ ] Sync `data_catalog.md` with the finalized schema once built

---

## Decisions Log

| Date | Decision | Reason |
|---|---|---|
| TBD | Hybrid structure: core logic in `src/*.py`, `notebooks/` reserved for EDA only | Keeps the pipeline testable and reusable instead of notebook-only |
| TBD | Source data ingested as CSV exports (not live JDBC connection) | Simpler local setup; matches Bronze layer design |
| TBD | Parquet as default storage format; Delta Lake optional | Avoids extra dependency unless ACID/time-travel features are actually needed |
| TBD | `dim_date` generated as a date spine (not derived from order dates) | Full calendar coverage, not just dates with orders |

---

## Open Questions

- [ ] What date range should the `dim_date` spine cover? (Match min/max `OrderDate` in `aw_sales_order_header`?)
- [ ] Will historization (SCD Type 2) be in scope for v1, or deferred to a later iteration?
- [ ] Confirm whether `region` and `group` in `dim_territory` should be the same column or split, per the "Rename based on your design" note.

---

## Issues & Resolutions

_Log problems encountered during development and how they were fixed._

| Issue | Resolution |
|---|---|
| _(none yet)_ | |

---

## Useful Commands

```bash
# Run a single job manually
python jobs/run_bronze_job.py
python jobs/run_silver_job.py
python jobs/run_gold_job.py

# Run tests
pytest tests/

# Launch Jupyter for exploration
jupyter notebook notebooks/
```
