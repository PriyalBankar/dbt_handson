# my_ecommerce_dbt — Multi-Domain dbt Project

A production-ready dbt project demonstrating multi-domain architecture with cross-domain model references, running on **Snowflake** via **dbt Cloud**.

---

## Architecture

```
SEEDS (raw CSV data)
    │
    ▼
STAGING  (views — clean & type raw data)
  commerce/staging/  →  stg_orders, stg_customers, stg_products
  finance/staging/   →  stg_transactions
    │
    ▼
INTERMEDIATE  (incremental/table — business logic)
  finance/intermediate/   →  int_finance_daily          ← anchor for cross-domain
  commerce/intermediate/  →  int_orders_enriched  ⬅️ refs int_finance_daily (CROSS-DOMAIN)
                          →  int_customer_360
    │
    ▼
EDW  (tables — star schema)
  commerce/edw/  →  fct_orders, dim_customer
  finance/edw/   →  fct_transactions
    │
    ▼
SEMANTIC  (views — BI-friendly names)
  commerce/semantic/  →  sem_revenue_metrics, sem_customer_kpis
  finance/semantic/   →  sem_finance_summary
```

### Cross-Domain Reference
`int_orders_enriched` (commerce) references `int_finance_daily` (finance) to enrich orders with FX rates. This is the key cross-domain dependency — changing `int_finance_daily` cascades into the commerce pipeline.

---

## Setup: dbt Cloud + Snowflake

### 1. Snowflake Setup
Run this SQL in Snowflake to create the required objects:

```sql
-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- Create database
CREATE DATABASE IF NOT EXISTS DBT_DEV;
CREATE DATABASE IF NOT EXISTS DBT_PROD;

-- Create role
CREATE ROLE IF NOT EXISTS TRANSFORMER;
GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE TRANSFORMER;
GRANT ALL ON DATABASE DBT_DEV TO ROLE TRANSFORMER;
GRANT ALL ON DATABASE DBT_PROD TO ROLE TRANSFORMER;

-- Create user (for dbt Cloud service account)
CREATE USER IF NOT EXISTS DBT_CLOUD_USER
  PASSWORD = 'your_secure_password'
  DEFAULT_ROLE = TRANSFORMER
  DEFAULT_WAREHOUSE = TRANSFORM_WH;

GRANT ROLE TRANSFORMER TO USER DBT_CLOUD_USER;
```

### 2. dbt Cloud Connection
In dbt Cloud → Settings → Connections → New Connection:
- **Type**: Snowflake
- **Account**: `your-account.region` (e.g. `xy12345.us-east-1`)
- **Database**: `DBT_DEV` (dev) / `DBT_PROD` (prod)
- **Warehouse**: `TRANSFORM_WH`
- **Role**: `TRANSFORMER`
- **Schema**: `PUBLIC` (dbt will create sub-schemas per layer)

### 3. Clone & Connect Repository
In dbt Cloud → New Project → connect your GitHub repo containing this project.

### 4. Load Seeds First
```bash
dbt seed
```
This loads the CSV files in `seeds/` into Snowflake as raw source tables.

### 5. Install Packages
```bash
dbt deps
```

---

## Running the Project

### Full Run (all models)
```bash
dbt build --selector all_domains
```

### By Domain
```bash
dbt build --selector domain_commerce
dbt build --selector domain_finance
```

### By Layer
```bash
dbt run --selector all_staging
dbt run --selector all_edw
dbt run --selector semantic_layer_refresh
```

### Cross-Domain Scenarios

| Scenario | Command |
|---|---|
| Finance FX rates changed | `dbt run --selector finance_daily_with_children` |
| Rebuild orders + all upstream | `dbt run --selector orders_enriched_with_parents` |
| Only changed models (CI) | `dbt build --selector slim_ci --defer --state ./prod_artifacts` |
| Nightly EDW refresh | `dbt run --selector nightly_incremental_edw --full-refresh` |
| Semantic layer only | `dbt run --selector semantic_layer_refresh` |

---

## dbt Cloud Job Setup

| Job Name | Selector | Schedule | Extra Flags |
|---|---|---|---|
| Nightly Full Build | `all_domains` | 02:00 UTC | — |
| Slim CI | `slim_ci` | On PR | `--defer --state` |
| Semantic Refresh | `semantic_layer_refresh` | 06:00 UTC | — |
| Finance Nightly | `nightly_incremental_edw` | 04:00 UTC | `--full-refresh` |

---

## Project Structure

```
my_ecommerce_dbt/
├── dbt_project.yml          # Project config + tag/schema assignments
├── selectors.yml            # Named selectors (the focus of this project)
├── packages.yml             # dbt_utils package
├── models/
│   ├── commerce/
│   │   ├── staging/         # stg_orders, stg_customers, stg_products
│   │   ├── intermediate/    # int_orders_enriched (cross-domain!), int_customer_360
│   │   ├── edw/             # fct_orders, dim_customer
│   │   └── semantic/        # sem_revenue_metrics, sem_customer_kpis
│   └── finance/
│       ├── staging/         # stg_transactions
│       ├── intermediate/    # int_finance_daily  ← cross-domain anchor
│       ├── edw/             # fct_transactions
│       └── semantic/        # sem_finance_summary
├── seeds/                   # raw_orders, raw_customers, raw_products, raw_transactions
├── macros/                  # utils.sql
└── tests/
```

---

## Selector Reference

All selectors are defined in `selectors.yml`. Key ones:

| Selector | Type | Purpose |
|---|---|---|
| `domain_commerce` | path | All commerce models |
| `domain_finance` | path | All finance models |
| `all_edw` | tag | All EDW models across domains |
| `cross_domain_models` | tag | Models tagged `cross_domain` |
| `orders_enriched_with_parents` | fqn + | int_orders_enriched + all upstream |
| `finance_daily_with_children` | fqn + | int_finance_daily + all downstream |
| `slim_ci` | state | Changed + new models only |
| `no_pii` | difference | Exclude PII-tagged models |
| `nightly_incremental_edw` | intersection | EDW incremental models only |
