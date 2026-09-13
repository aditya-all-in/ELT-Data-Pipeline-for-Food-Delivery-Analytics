# End-to-End ELT Data Pipeline for Food Delivery Analytics

An end-to-end, production-grade batch ELT data pipeline simulating large-scale food delivery operations across millions of orders, catalog items, and unstructured customer feedback. Built with **Snowflake**, **dbt Core**, and **Apache Airflow**, following the **Medallion Architecture** and **Kimball Dimensional Modeling**.

---

## 1. Architecture Overview

```
                          [ DATA LAKE ]
                       AWS S3 Raw Storage
                                │
                                ▼(Keyless Storage Integration)
                       [ BRONZE LAYER ]
                      Snowflake ZOMATO.RAW
                  (External Stages & COPY INTO)
                                │
                                ▼  (dbt Models - Silver)
                       [ SILVER LAYER ]
                    Snowflake ZOMATO.STAGING
             (1:1 Views, Defensive Typing, City Regex Parsing)
                                │
                                ▼  (dbt Models - Gold)
                        [ GOLD LAYER ]
                    Snowflake ZOMATO.MARTS
      ┌─────────────────────────┴────────────────────────┐
      ▼                                                  ▼
[ Fact Tables ]                                 [ Conformed Dimensions ]
- fct_orders (Incremental Merge, 3-day lookback) - dim_customers (Demographic Tiers)
- fct_reviews (Order & Merchant Enriched)        - dim_restaurants (Merchant Profiles)
                                                - dim_food (Catalog Taxonomy)
                                                - dim_date (Generated Spine)
                                │
                                ▼
                   [ ANALYTICS & BI SERVING ]
         Streamlit Intelligence App & KPI Dashboards
```

---

## 2. Tech Stack

- **Cloud Data Warehouse:** Snowflake (`ZOMATO_WH`, Virtual Warehouse with auto-suspend FinOps controls)
- **Object Storage / Data Lake:** Amazon S3 (Raw partitioned bucket landing zone)
- **Data Transformation:** dbt Core (`dbt-snowflake`)
- **Workflow Orchestration:** Apache Airflow 2.10 (Containerized via Docker Compose)
- **Data Modeling Paradigm:** Kimball Dimensional Modeling (Star Schema)
- **Languages & Frameworks:** SQL, Python 3.10+, Jinja, Bash

---

## 3. Repository Structure

```
├── snowflake/                         # Snowflake Infrastructure as Code
│   ├── 01_setup.sql                   # Warehouse, database, schemas, RBAC
│   ├── 02_storage_integration.sql     # Keyless AWS S3 storage integration
│   ├── 03_stage_and_formats.sql       # CSV file formats and external stages
│   ├── 04_raw_tables.sql              # Bronze DDL matching source landing schemas
│   └── 05_copy_into.sql               # Bulk COPY INTO loading pipelines
├── zomato/                            # dbt Core Transformation Project
│   ├── dbt_project.yml                # Project configurations and schema definitions
│   ├── macros/
│   │   └── generate_schema_name.sql   # Custom schema resolution macro
│   └── models/
│       ├── staging/                   # Silver Layer (1:1 Views + Schema Validations)
│       │   ├── _sources.yml           # Raw source declarations
│       │   ├── schema.yml             # Primary key and not-null tests for staging
│       │   ├── stg_orders.sql
│       │   ├── stg_order_items.sql
│       │   ├── stg_restaurants.sql
│       │   ├── stg_users.sql
│       │   ├── stg_food.sql
│       │   ├── stg_menu.sql
│       │   └── stg_reviews.sql
│       └── marts/                     # Gold Layer (Star Schema Dimensions & Facts)
│           ├── schema.yml             # Referential integrity, unique, and accepted value tests
│           ├── fct_orders.sql         # Incremental Fact (Merge strategy, 3-day lookback)
│           ├── fct_reviews.sql        # Review Fact enriched with merchant attributes
│           ├── dim_customers.sql      # Customer Dimension with age segmentation
│           ├── dim_restaurants.sql    # Restaurant Dimension with cuisine & ratings
│           ├── dim_food.sql           # Food Catalog Dimension
│           └── dim_date.sql           # Calendar Spine Dimension (2024–2026)
├── airflow/                           # Containerized Orchestration Engine
│   ├── dags/
│   │   └── zomato_batch_pipeline.py   # Production DAG with quality validation gates
│   ├── Dockerfile                     # Custom Airflow image with dbt-snowflake
│   ├── docker-compose.yaml            # Multi-service local cluster definition
│   └── .env.example                   # Template environment configurations
└── README.md                          # Platform documentation
```

---

## 4. Medallion Architecture Implementation

### Bronze Layer (`ZOMATO.RAW`)
- **Purpose:** High-throughput, raw ingest layer mirroring source operational schemas.
- **Pattern:** Populated via Snowflake external stages linked to S3 via storage integrations.
- **Storage:** Standard permanent tables using `COPY INTO` with `ON_ERROR = 'SKIP_FILE'`.

### Silver Layer (`ZOMATO.STAGING`)
- **Purpose:** Cleansed, typed, 1:1 representation of operational source data.
- **Materialization:** `view` (zero redundant storage cost in Snowflake).
- **Transformation Patterns:**
  - Defensive type-casting using `try_to_number()`, `try_to_decimal()`, and `try_to_date()` to prevent malformed string rows from crashing downstream builds.
  - Regular expression parsing to clean city names and strip localized currency markers (`₹`).
  - Column aliasing to standardize identifiers across domains (e.g., `user_id` $\rightarrow$ `customer_id`, `r_id` $\rightarrow$ `restaurant_id`).

### Gold Layer (`ZOMATO.MARTS`)
- **Purpose:** Production-ready analytics layer organized as a Kimball Star Schema for business intelligence and ML feature stores.
- **Materialization:** `table` for dimensions and `incremental` for fact tables.

---

## 5. Data Model & Grain

| Entity | Layer | Grain | Primary Key | Materialization | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `fct_orders` | Gold | 1 row per order | `order_id` | `incremental` | Core order revenue, tax, discount, and status. |
| `fct_reviews` | Gold | 1 row per review | `review_id` | `table` | Customer ratings and unstructured feedback text. |
| `dim_customers` | Gold | 1 row per user | `customer_id` | `table` | Demographics and age tiers (`Gen Z`, `Millennial`, etc.). |
| `dim_restaurants`| Gold | 1 row per restaurant | `restaurant_id` | `table` | Location, cuisine, and average rating metadata. |
| `dim_food` | Gold | 1 row per menu item | `food_id` | `table` | Food catalog items and dietary classification. |
| `dim_date` | Gold | 1 row per calendar day | `date_day` | `table` | Calendar spine supporting time-series slicing. |

---

## 6. Incremental Fact Strategy (`fct_orders`)

Processing millions of daily orders via full table refreshes causes unnecessary Snowflake compute costs. `fct_orders` is engineered using an incremental merge pattern:

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
        -- 3-day lookback window handles late-arriving records and retroactive status updates
        where order_timestamp >= (select dateadd(day, -3, max(order_timestamp)) from {{ this }})
    {% endif %}
)
```

- **Merge Strategy:** Deduplicates records against `order_id`, inserting new arrivals while updating existing orders whose status transitioned to `Delivered` or `Cancelled`.
- **Late-Arriving Data:** The 3-day lookback window prevents dropped records caused by network latency or settlement delays without requiring full historical scans.

---

## 7. Data Quality & Integrity Testing

Data quality gates are built directly into the dbt transformation DAG:
- **Uniqueness & Non-Null:** Applied to all primary keys (`order_id`, `customer_id`, `restaurant_id`, `food_id`).
- **Referential Integrity:** `fct_orders.customer_id` and `fct_orders.restaurant_id` enforce relationship tests to conformed dimensions, ensuring zero orphaned financial records.
- **Accepted Values:** Guarantees standard domain values for `order_status` (`Delivered`, `Cancelled`, `Pending`, `In Progress`) and dietary classifications (`Veg`, `Non-Veg`, `Egg`).

---

## 8. Pipeline Execution Guide

### Step 1: Initialize Snowflake
Execute scripts in `snowflake/` in order within Snowflake Snowsight:
```sql
-- 1. Setup Role, Warehouse, Database, Schemas
-- 2. Setup S3 Storage Integration & External Stage
-- 3. Ingest Bronze tables via COPY INTO
```

### Step 2: Run dbt Transformations Locally
```bash
cd zomato

# Verify connection
dbt debug

# Execute Staging transformations and tests
dbt run --select staging
dbt test --select staging

# Execute Gold Marts transformations and tests
dbt run --select marts
dbt test --select marts

# Generate documentation and interactive lineage graph
dbt docs generate
dbt docs serve
```

### Step 3: Run Orchestration via Docker
```bash
cd airflow
docker compose up -d
```
Access the Airflow Web UI at `http://localhost:8080` (Default credentials: `admin` / `admin`) to view and monitor the `zomato_daily_batch_pipeline` DAG.

---

## 9. Visual Artifacts & Lineage

<!-- PLACEHOLDER: Insert dbt Lineage Graph Screenshot Here -->
> *Figure 1: dbt DAG Lineage showing Bronze Sources -> Silver Staging Views -> Gold Marts.*

<!-- PLACEHOLDER: Insert Airflow DAG Graph View Screenshot Here -->
> *Figure 2: Apache Airflow DAG demonstrating quality gates between staging and marts layers.*

---

## 10. Planned Future Improvements

1. **dbt-expectations Integration:** Introduce statistical distribution tests on `sales_amount` and `delivery_time_min` to detect outlier delivery anomalies.
2. **SCD Type 2 Dimension Tracking:** Implement dbt snapshots on `dim_restaurants` to track menu price inflation and cuisine shifts over time.
3. **Automated Alerting Webhooks:** Integrate Slack/PagerDuty webhooks into Airflow failure callbacks to notify on-call engineers of pipeline breaches.
