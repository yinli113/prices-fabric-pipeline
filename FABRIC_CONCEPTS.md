# PricesAPI to Microsoft Fabric Pipeline Concepts

This document explains key concepts and design decisions for processing raw e-commerce data (from PricesAPI) using Microsoft Fabric's Real-Time Intelligence (KQL / Eventhouse).

## 1. Why are Prices so Different for the Same Product ID?

When querying the API for a specific product ID (e.g., `3669` for a "PlayStation 5 Console"), you might see prices ranging from $20 to $1,299.

### The Cause: Scraped Search Results
The `product_id` is unique to the API provider's catalog entry. However, when the API fetches live prices, its bots scrape the search results of retailers like Amazon or eBay using that product's keyword. 
Retailer search engines do not just return the console; they return highly relevant accessories (controllers, skins, cases) and sometimes inflated scalper bundles. All of these get scooped up and grouped under the single `product_id`.

### The Solution: The "Gold" Layer
In a Medallion Architecture (Bronze -> Silver -> Gold), we solve this using data engineering in KQL. Instead of trusting the raw data, we build a **Gold View** that filters out the noise:

```kusto
.create-or-alter function gold_competitor_gaps() {
    silver_price_history()
    | extend search_title = tolower(offer_title)
    // 1. Keyword exclusion (drop accessories)
    | where search_title !contains "controller"
        and search_title !contains "skin"
        and search_title !contains "refurb"
    // 2. Price bounds (drop $20 cables and $2,000 scalpers)
    | where price >= 400 and price <= 1200
}
```
Your Power BI dashboard then connects to this clean `gold_competitor_gaps` view, ensuring business users only see legitimate console prices.

---

## 2. How KQL Handles Streaming Data vs. Spark

If you are used to Apache Spark Structured Streaming, you know that Spark uses **Checkpoints** (`checkpointLocation`) to track the exact Kafka offset it last processed. This ensures the streaming query only processes *new* data.

### KQL is an Append-Only Database
KQL (Kusto) in Microsoft Fabric works completely differently. It is not a stream processing engine; it is a highly-optimized analytical database.
When Fabric Eventstream ingests data into a KQL table, it permanently appends new rows to the disk. There is no "checkpoint" that moves forward. If you query the table, you query *all* the historical data.

### How to handle Time in KQL
Because KQL is designed to hold billions of rows of telemetry data, it is hyper-optimized for time-based filtering at query time. You handle streaming state in three ways:

**A. The "Latest State" (Dashboard View)**
To show only the current, newest price for a product, you query the entire table but ask KQL to return only the row with the most recent ingestion time per seller:
```kusto
silver_price_history()
| summarize arg_max(ingestion_time, *) by product_id, seller
```

**B. The "Time Window" (Alerting View)**
To see only data that arrived recently (e.g., for a flash sale alert), you filter relative to `now()`:
```kusto
gold_competitor_gaps()
| where ingestion_time > ago(1h)
```
*Note: KQL physically partitions data by time, so `ago(1h)` queries are blazingly fast because KQL ignores yesterday's files on the disk entirely.*

**C. Materialized Views (The "Checkpoint" Equivalent)**
If running `arg_max` on a massive historical table is too slow for a live dashboard, KQL offers **Materialized Views**. 
```kusto
.create materialized-view LatestPrices on table raw_prices_stream
{
    raw_prices_stream
    | summarize arg_max(ingestion_time, *) by product_id
}
```
A Materialized View acts similarly to a Spark Checkpoint. Kusto runs a background process that continuously reads new data and updates the view. When your dashboard queries `LatestPrices`, the result is pre-computed and instant.

---

## 3. Where is my Code? (Databricks vs. Microsoft Fabric)

If you are coming from Databricks, you are used to seeing a clear list of Python or SQL Notebooks in your workspace that explicitly orchestrate the Bronze → Silver → Gold transformations (e.g., a notebook named `01_Ingest_Bronze`, another `02_Clean_Silver`).

Microsoft Fabric supports the exact same Notebook-driven approach using Fabric Spark. However, in our **Real-Time Intelligence (KQL)** architecture, the code lives in different places:

### A. The Real-Time (KQL) Approach (What we built)
Because we used KQL Functions (`silver_price_history`, `gold_competitor_gaps`) to create live views over the data, there are no "Notebooks" executing batches. The transformation happens on-the-fly when the dashboard queries the data.

**Where to find the code:**
1. Open your **KQL Database** (`price_eventhouse`).
2. In the left-hand navigation pane, expand your database.
3. Expand the **Functions** folder.
4. You will see `silver_price_history` and `gold_competitor_gaps` listed there.
5. Click on any function. A query window will open showing the exact KQL code used to create that layer. You can edit the code and click **Run** (with `.create-or-alter function`) to update your logic.

### B. The Batch (Spark) Approach (The Databricks Way)
If you prefer the Databricks workflow, Microsoft Fabric has a **Data Engineering** workload that is virtually identical to Databricks:

1. Instead of a KQL Database, you route your Eventstream to a **Fabric Lakehouse** (Delta Parquet files).
2. You click **New Notebook** in your workspace.
3. You write PySpark or Spark SQL code to read the Bronze Delta table, transform it, and write it to a Silver Delta table.
4. You use **Fabric Data Factory (Pipelines)** to schedule those notebooks to run on a cadence (e.g., every hour).

**Which should you use?**
* Use **KQL & Functions** (what we did) when you need sub-second latency, streaming ingestion, and instant dashboard updates without paying for Spark compute clusters to spin up.
* Use **Spark Notebooks & Lakehouses** when doing heavy machine learning, massive historical backfills, or complex joins across dozens of systems.

---

## 3. Where is my Code? (Databricks vs. Microsoft Fabric)

If you are coming from Databricks, you are used to seeing a clear list of Python or SQL Notebooks in your workspace that explicitly orchestrate the Bronze → Silver → Gold transformations (e.g., a notebook named `01_Ingest_Bronze`, another `02_Clean_Silver`).

Microsoft Fabric supports the exact same Notebook-driven approach using Fabric Spark. However, in our **Real-Time Intelligence (KQL)** architecture, the code lives in different places:

### A. The Real-Time (KQL) Approach (What we built)
Because we used KQL Functions (`silver_price_history`, `gold_competitor_gaps`) to create live views over the data, there are no "Notebooks" executing batches. The transformation happens on-the-fly when the dashboard queries the data.

**Where to find the code:**
1. Open your **KQL Database** (`price_eventhouse`).
2. In the left-hand navigation pane, expand your database.
3. Expand the **Functions** folder.
4. You will see `silver_price_history` and `gold_competitor_gaps` listed there.
5. Click on any function. A query window will open showing the exact KQL code used to create that layer. You can edit the code and click **Save** (or run `.create-or-alter function`) to update your logic.

### B. The Batch (Spark) Approach (The Databricks Way)
If you prefer the Databricks workflow, Microsoft Fabric has a **Data Engineering** workload that is virtually identical to Databricks:

1. Instead of a KQL Database, you route your Eventstream to a **Fabric Lakehouse** (Delta Parquet files).
2. You click **New Notebook** in your workspace.
3. You write PySpark or Spark SQL code to read the Bronze Delta table, transform it, and write it to a Silver Delta table.
4. You use **Fabric Data Factory (Pipelines)** to schedule those notebooks to run on a cadence (e.g., every hour).

**Which should you use?**
* Use **KQL & Functions** (what we did) when you need sub-second latency, streaming ingestion, and instant dashboard updates without paying for Spark compute clusters to spin up.
* Use **Spark Notebooks & Lakehouses** when doing heavy machine learning, massive historical backfills, or complex joins across dozens of external systems.
