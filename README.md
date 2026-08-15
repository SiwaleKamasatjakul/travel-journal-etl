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