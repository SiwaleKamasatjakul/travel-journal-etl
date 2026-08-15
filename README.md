# Travel Journal Platform

A cross-platform (Web & Mobile) interactive travel journal application that enables users to capturing trip stops, schedules, activities, ratings, and total distance traveled.

You can view the project at this link

https://travel-journal-one-orpin.vercel.app/

## 1. System & Architecture Overview

- **Database Layer:** Managed PostgreSQL hosted on **Supabase**.
- **Storage Layer:** Supabase S3 buckets for media and image storage.
- **Data Pipelines & ETL:**
    - **Batch Orchestration:** Databricks Jobs (Scheduled triggers)
    - **Pipeline Processing:** Delta Live Tables (DLT) & PySpark
    - **Pipeline Testing:** Automated test runs via Databricks Jobs and DLT pipeline updates to validate incremental loading logic prior to production release.
- **CI/CD & Automation:** GitHub Actions for automated deployment and testing workflows.

## 2. Relational Database Schema

The core application data structure tracks user accounts, media mappings, and user preferences:

## 3. Data Engineering & ETL Pipeline

### Source & Ingestion Strategy

- **Data Origin:** Ingested from the Supabase application backend via secure REST APIs.
- **Movement Pattern:** Batch ingestion scheduled and executed on **Databricks**.

### Data Modeling & Storage

- **Analytical Architecture:** Raw operational transactional data is extracted and transformed into a **Star Schema** (Fact and Dimension tables) optimized for analytical reporting and journey analytics (e.g., aggregations on user activity, distance, ratings, and temporal trends).
- **Target Storage:** Transformed models are organized and stored in **Databricks Unity Catalog**, using three-level namespace governance (`catalog.schema.table`) to separate raw, silver, and analytical star schema layers.

### Data Quality & Production Readiness

- **Quality Assurance:** Automated validation suites, schema checks executed within the Databricks notebook/job environment prior to writing to target tables.
- **CI/CD & Operations:** Fully automated build and deployment workflows powered by **GitHub Actions** for continuous integration, code checks, and automated pipeline updates.


## 4. Gold Layer Analytics & Data Modeling Results

**Data Range:** June 1, 2026 – August 15, 2026

**Catalog:** `travel_journal_catalog.gold`

**Description:** This section documents the SQL queries and analytical models developed following the completion of the Gold layer data modeling phase.

## 1. Top 10 Most-Liked Posts

### Overview

Identifies the top 10 most popular trip posts based on overall engagement (likes).

### Tables Used

- `gold.fact_trip_post` (Post Fact)
- `gold.fact_post_like` (Like Fact)

### SQL Implementation

SQL

```
-- Top 10 most-liked posts
SELECT
    trip_post.trip_post_id,
    trip_post.trip_name,
    SUM(like.like_count) AS total_likes
FROM travel_journal_catalog.gold.fact_trip_post trip_post
INNER JOIN travel_journal_catalog.gold.fact_post_like like
    ON trip_post.trip_post_id = like.trip_post_id
GROUP BY
    trip_post.trip_post_id,
    trip_post.trip_name
ORDER BY
    total_likes DESC
LIMIT 10;
```

## 2. Top Activities Analysis

### Overview

Ranks the top activities engaged in by users across all posted trips.

### Tables Used

- `gold.bridge_trip_post_activity` (Trip-Activity Bridge)
- `gold.dim_activity_code` (Activity Dimension)

### SQL Implementation

SQL

```
-- Top activities breakdown
SELECT
    trip_post_act.activity_code,
    activity_code.activity_name,
    COUNT(*) AS activity_count
FROM travel_journal_catalog.gold.bridge_trip_post_activity trip_post_act
INNER JOIN travel_journal_catalog.gold.dim_activity_code activity_code
    ON trip_post_act.activity_code = activity_code.activity_code
GROUP BY
    trip_post_act.activity_code,
    activity_code.activity_name
ORDER BY
    activity_count DESC
LIMIT 10;
```

## 3. Posting Activity & Country Volume by Month

### Overview

Tracks monthly trends in posting activity broken down by destination country.

### Tables Used

- `gold.fact_trip_post` (Post Fact)
- `gold.dim_date` (Date Dimension)
- `gold.bridge_trip_post_country` (Trip-Country Bridge)
- `gold.dim_country_code` (Country Dimension)

### SQL Implementation

SQL

```
-- Monthly posting volume per country
SELECT
    dim_date.month,
    country_code.country_name,
    COUNT(*) AS country_count
FROM travel_journal_catalog.gold.fact_trip_post trip_post
INNER JOIN travel_journal_catalog.gold.dim_date dim_date
    ON trip_post.post_date_id = dim_date.date_id
INNER JOIN travel_journal_catalog.gold.bridge_trip_post_country country
    ON trip_post.trip_post_id = country.trip_post_id
INNER JOIN travel_journal_catalog.gold.dim_country_code country_code
    ON country.country_code = country_code.country_code
GROUP BY
    dim_date.month,
    country.country_code,
    country_code.country_name
ORDER BY
    dim_date.month ASC,
    country_code.country_name ASC;
```

## 4. Average Trip Rating Grouped by Country and Activity

### Overview

Calculates average user ratings across specific travel activities within each destination country.

### Tables Used

- `gold.fact_trip_post` (Post Fact)
- `gold.bridge_trip_post_country` (Trip-Country Bridge)
- `gold.dim_country_code` (Country Dimension)
- `gold.bridge_trip_post_activity` (Trip-Activity Bridge)
- `gold.dim_activity_code` (Activity Dimension)

### SQL Implementation

```
-- Average trip ratings by country and activity
SELECT
    country_code.country_name,
    activity_code.activity_name,
    AVG(trip_post.rating) AS avg_rating,
    COUNT(DISTINCT trip_post.trip_post_id) AS total_trips
FROM travel_journal_catalog.gold.fact_trip_post trip_post
INNER JOIN travel_journal_catalog.gold.bridge_trip_post_country trip_country
    ON trip_post.trip_post_id = trip_country.trip_post_id
INNER JOIN travel_journal_catalog.gold.dim_country_code country_code
    ON trip_country.country_code = country_code.country_code
INNER JOIN travel_journal_catalog.gold.bridge_trip_post_activity trip_act
    ON trip_post.trip_post_id = trip_act.trip_post_id
INNER JOIN travel_journal_catalog.gold.dim_activity_code activity_code
    ON trip_act.activity_code = activity_code.activity_code
GROUP BY
    country_code.country_name,
    activity_code.activity_name
ORDER BY
    avg_rating DESC;
```

## 🛠️ 5. ETL Pipeline: Step-by-Step Implementation

### Architecture Overview

```text
[ Supabase PostgreSQL ]
       │
       │ (REST API / JDBC Extraction)
       ▼
[ Azure Data Factory ] ◄────── Sync Secrets ──────► [ Azure Key Vault ]
       │
       │ (Triggers PySpark Processing)
       ▼
[ Databricks Jobs (Auto Loader) ]
       │
       │ (Ingests & Governs via Unity Catalog)
       ▼
[ ADLS Gen2 Storage ]
   ├── /bronze/{table}  ──► Incremental raw ingestion + Checkpoint state
   ├── /silver/{table}  ──► Cleaned, dynamic CDC data & quality rules
   └── /gold/{table}    ──► Analytical Star Schema (Facts, Dims, Bridges)
```

## Step 1: Azure Infrastructure Provisioning

### 1. Resource Group & Storage

1. Create an **Azure Resource Group** 
2. Create an **Azure Data Lake Storage Gen2** account (Hierarchical Namespace enabled).
3. Create container named `lakehouse`.
4. Create the target landing directory structure inside the container:
    - `/raw_landing/`
    - `/bronze/`
    - `/silver/`
    - `/gold/`

### 2. Azure Key Vault Configuration

1. Create an **Azure Key Vault** (e.g., `kv-travel-journal`).
2. Navigate to **Secrets** and add your Supabase credentials:
    - `supabase-url`: Your Supabase API REST URL or JDBC Connection String.
    - `supabase-key`: Service role key / API secret.
    - `storage-account-key`: ADLS Gen2 access key.

### 3. Grant Databricks & ADF Access to Key Vault

- In Key Vault **Access Policies** (or Azure RBAC), grant **Key Vault Secrets User** role to both:
    - Azure Data Factory Managed Identity.
    - Azure Databricks Managed Identity / Service Principal.

## Step 2: Databricks Secret Scope Setup

To securely reference Key Vault credentials directly in PySpark without hardcoding, link Key Vault to Databricks using an **Azure Key Vault-backed Secret Scope**:

1. Open Databricks workspace and go to: `https://<databricks-instance>#secrets/createScope`.
2. Set **Scope Name**: `supabase-scope`.
3. Provide the **DNS Name** and **Resource ID** from your Azure Key Vault properties page.

You can now call secrets in PySpark:
```
supabase_url = dbutils.secrets.get(scope="supabase-scope", key="supabase-url")
supabase_key = dbutils.secrets.get(scope="supabase-scope", key="supabase-key")
```
## Step 3: Azure Data Factory (ADF) Configuration

### 1. Create Linked Services

- **Supabase Linked Service:** HTTP REST API endpoint or PostgreSQL JDBC connection using Azure Key Vault secrets.
- **Databricks Linked Service:** Point to your Azure Databricks workspace authentication via Personal Access Token (PAT) or Azure AD Managed Identity.
- **Azure Key Vault Linked Service:** Point to your Key Vault.

### 2. ADF Orchestration Pipeline

Create a pipeline using a **ForEach Activity** to iterate over your target tables dynamically.

#### Pipeline Parameters

- `table_list` (Array):

## Step 4: Databricks Incremental Bronze Ingestion Script

This PySpark script uses **Databricks Auto Loader (`cloudFiles`)** for incremental processing, schema inference, progress checkpointing, and date-based partitioning.

→ source code 

## Step 5: Partitioning Strategy & Incremental Engine

### Why Data Partitioning?

For travel post metrics (`trip_posts`, `trip_stops`), volume grows continuously over time. Partitioning by date (`year`/`month`/`day` or `created_at_date`) provides three major benefits:

1. **Query Pruning:** Queries targeting specific date ranges skip scanning unrelated files, significantly reducing latency and compute costs.
2. **File Organization:** Prevents single large directories by balancing output into structured partition directories.
3. **Seamless Scaling:** Handles high write rates as daily travel activity scales up.

### Incremental Execution Flow (`availableNow=True`)

- **Schema Tracking (`schemaLocation`):** Auto Loader records column structure. If Supabase alters schema in future releases, updates are caught automatically without pipeline failures.
- **Offset Tracking (`checkpointLocation`):** Tracks processed files. Subsequent runs skip already ingested records to prevent duplicate processing.
- **Batch Micro-burst (`availableNow=True`):** Processes all pending micro-batches as a single batch, updating the checkpoint before gracefully shutting down cluster resources.

## Step 6: ADF Trigger & Scheduling

1. In Azure Data Factory, add a **Schedule Trigger** to the pipeline .
2. Set execution frequency to **Daily at 00:00 UTC** (or hourly depending on travel update SLAs).

# Step 7: Silver Layer Processing (Data Cleaning, Quality & Standardization)

The Silver layer transforms raw, append-only Bronze data into clean, deduplicated Delta tables. Each table in the pipeline receives a dedicated PySpark processing notebook to handle table-specific schema validations and cleaning rules.

## Silver Layer Architecture & Storage

- **Storage Target:** ADLS Gen2 Delta format (`abfss://silver@databricktraveljournal.dfs.core.windows.net/<table_name>`)
- **Governance:** Registered under **Databricks Unity Catalog** (`workspace.silver.<table_name>`)
- **Execution Environment:** Databricks compute clusters.
```
[ Bronze Layer (Parquet/Delta) ]
               │
               ▼
┌────────────────────────────────────────────────────────┐
│               SILVER NOTEBOOK PIPELINE                 │
├────────────────────────────────────────────────────────┤
│ 1. Schema Enforcement & Data Quality Checks            │
│    • Extract missing created_at from _rescued_data     │
│    • Trim string whitespace                            │
│    • Correct seeded year errors (2024 ➔ 2026)          │
│    • Enforce Primary Key Non-Null rules               │
├────────────────────────────────────────────────────────┤
│ 2. Deduplication via Window Functions (QUALIFY Pattern)│
│    • Partition by Primary Key, Order by latest date    │
├────────────────────────────────────────────────────────┤
│ 3. Unity Catalog Delta Target Write                   │
│    • Write mode: Overwrite / Merge into Unity Catalog  │
└────────────────────────────────────────────────────────┘
               │
               ▼
[ Silver Layer (Unity Catalog Managed Delta Tables) ]
```
## Processing Steps Explained

### 1. Data Quality & Cleaning

- **Rescued Data Extraction:** When Auto Loader encounters unparseable or corrupted payload columns in Bronze, it pushes them into `_rescued_data`. We inspect `_rescued_data` to rescue missing `created_at` timestamps using `get_json_object`.
- **Text Normalization:** Strip leading and trailing whitespace across text fields (`trim()`).
- **Seeded Year Correction:** Auto-generated mock/seeded data incorrectly generated timestamps set in `2024`. We programmatically adjust these date values forward to `2026` using PySpark date manipulation functions.
- **Non-Null Enforcement:** Reject/filter out invalid records missing core identifier keys (e.g., `id`, `user_id`, `post_id`).
