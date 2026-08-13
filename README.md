# NYC Taxi Demand and Revenue Analytics

**MGMT 59900 – Big Data Analytics in the Cloud**
**Purdue University Online MSBA – Summer 2026**
**Group 9:** Eugenia Camacho, Nathan Healey Eversole, Oguz Isgoren

## Project Overview

This project aims to analyze the data in January 2025 concerning the rides made through the Yellow taxis in New York City in order to make fleet managers and transportation planners aware of the different scenarios under which the riders will need a taxi and when they will specifically need one.

This project consists of a batch processing pipeline on AWS in which the data will be stored on Amazon S3 in the Bronze table, data cleansing in the Silver table, rejection of the trip in the Rejected table, and finally the generation of data based on the Gold tables.

## Business Problem

Taxi availability is most valuable when the taxis are in places where the demand and potential revenues really happen. The study aims to answer the following three questions:

* At what time of day is taxi demand the greatest?
* Which pickup zones produce the largest number of trips and revenues?
* How does fare and tip amount differ by payment method?

The findings of this study will help to make decisions on positioning of taxis.

## Dataset

This study employs the NYC TLC Yellow Taxi Trip Record Data from January 2025 and the TLC Taxi Zone Lookup Table.

Total number of bronze trips: **3,475,226**
Type of trip data: **Parquet**
Type of zone lookup data: **CSV**

**Essential fields: Pickup and Dropoff timestamps; Pickup and Dropoff location IDs; Trip distance; Number of passengers; Mode of payment; Fare; Tip; Total amount.** 

The dataset is concerning the yellow medallion taxis only and does not convey information about Uber, Lyft and green taxis, etc.
## AWS Architecture

The project uses a serverless AWS batch architecture:

1. **Amazon S3 – Bronze Layer**
   Stores the original taxi trip data and TLC zone lookup data in separate `trips/` and `lookup/` prefixes.

2. **AWS Glue Data Catalog**
   Glue crawlers inspect the Bronze data and register the `bronze_trips` and `bronze_lookup` tables in the `nyc_taxi_db` database.

3. **Amazon Athena**
   Athena is used both for data-quality profiling and as the SQL transformation engine.

4. **Amazon S3 – Silver Layer**
   Stores validated trip records that pass the project's data-quality checks.

5. **Amazon S3 – Rejected Layer**
   Preserves records that fail validation and keeps rejection reasons for auditing.

6. **Amazon S3 – Gold Layer**
   Stores pre-aggregated analytical tables used for the final business analysis.

The final architecture diagram is available in:

`diagrams/architecture_diagram.png`

## Data Quality and Validation

Initial Athena profiling of the **3,475,226 Bronze records** identified:

* **2,051** records with invalid timestamp order
* **90,893** records with negative trip distance
* **145,516** records with zero fare amounts
* **63,596** records with zero total amounts

The final transformation produced:

* **Silver:** 2,791,161 validated records
* **Rejected:** 684,065 records
* **Bronze:** 3,475,226 records

A reconciliation control verifies:

**Silver + Rejected = Bronze**

`2,791,161 + 684,065 = 3,475,226`

An earlier version of the rejection query mishandled NULL passenger-count values and caused **412,619 records** to fall into neither output. The reconciliation check exposed the issue, and the SQL logic was corrected before final analysis.

Invalid or missing passenger counts accounted for **436,887** rejected records, the largest individual rejection category.

## Gold Tables

Three Gold tables were created from the validated Silver data:

### `gold_hourly_demand`

Aggregates taxi demand by date and hour.

Used to answer:

> When are more taxis needed?

### `gold_zone_performance`

Aggregates trip volume, revenue, fares, tips, and distance by pickup zone.

Used to answer:

> Where are demand and revenue opportunities concentrated?

### `gold_payment_analysis`

Aggregates trip count, fare, and recorded tipping behavior by payment method.

Used to answer:

> How do fares and recorded tips differ by payment type?

## Key Findings

Several important patterns emerged from the final analysis:

* **5 PM** was the busiest hour with **205,245 trips**.
* The period from **3 PM to 9 PM accounted for approximately 46% of all trips**.
* Revenue per trip peaked at **5 AM at $36.92**, despite low trip volume.
* **Upper East Side South** had the highest validated pickup volume with **147,549 trips**.
* **Midtown Center** followed with **146,192 trips**.
* **JFK Airport generated approximately $10.1 million in revenue**.
* JFK pickups averaged approximately **$79.22 per trip**.
* Airports represented approximately **7.6% of trips but 21.7% of revenue**.
* Card and cash average fares were nearly identical: **$17.61 vs. $17.57**.

These findings suggest that a single dispatch strategy should not be used throughout the entire day. Afternoon and evening supply should emphasize Manhattan's high-volume zones, while early-morning positioning can place more weight on airport demand.

## Cost Control

The project uses serverless AWS services to avoid paying for idle compute resources.

Cost-control measures include:

* Amazon S3 for low-cost object storage
* AWS Glue for cataloging
* Amazon Athena for serverless SQL processing
* Parquet storage for analytical layers
* Pre-aggregated Gold tables to reduce repeated scans
* Resource cleanup after final validation

The entire project infrastructure was estimated to cost **less than $0.10**.

As an example of the benefit of pre-aggregation, a query against `gold_hourly_demand` scanned only **7.90 KB** instead of repeatedly scanning the complete Silver dataset.

## Repository Structure

```text
nyc-taxi-demand-revenue-analytics/
│
├── README.md
│
├── charts/
│   ├── chart_demand_by_hour.png
│   ├── chart_payment.png
│   └── chart_zone_revenue.png
│
├── data/
│   ├── borough_rollup.csv
│   ├── demand_by_hour.csv
│   ├── payment_analysis.csv
│   ├── top_zones_by_revenue.csv
│   └── top_zones_by_volume.csv
│
├── diagrams/
│   └── architecture_diagram.png
│
├── screenshots/
│   ├── 01_s3_bucket_root.png
│   ├── 02_s3_bronze_prefixes.png
│   ├── 03_s3_lookup.png
│   ├── 04_s3_trips.png
│   ├── 05_glue_crawler_run.png
│   ├── 06_bronze_trips_schema.png
│   ├── 07_athena_row_count.png
│   ├── 08_data_quality_profiling.png
│   ├── 09_reconciliation_pass.png
│   ├── 10_rejection_breakdown.png
│   ├── 11_gold_hourly_demand.png
│   ├── 12_top_zones.png
│   └── 13_payment_analysis.png
│
└── sql/
    └── segmentB_pipeline.sql
```

## How to Reproduce the Pipeline

To reproduce the project in another AWS account:

1. Create an Amazon S3 bucket for the project.
2. Create Bronze, Silver, Rejected, Gold, and Athena-results prefixes.
3. Upload the January 2025 Yellow Taxi Parquet file and TLC Taxi Zone Lookup CSV to the Bronze layer.
4. Create the `nyc_taxi_db` AWS Glue database.
5. Configure and run a Glue crawler against the Bronze data.
6. Confirm that `bronze_trips` and `bronze_lookup` are registered in the Glue Data Catalog.
7. Open Amazon Athena and configure an S3 query-results location.
8. Review `sql/segmentB_pipeline.sql` and replace any account-specific S3 bucket paths with paths for the target AWS account.
9. Run the profiling, Silver, Rejected, reconciliation, and Gold-table SQL statements in the documented order.
10. Confirm that Silver plus Rejected equals the original Bronze record count before using the Gold outputs for analysis.

## Evidence and Results

The `screenshots/` directory documents the implementation from the S3 data-lake structure through Glue cataloging, Athena profiling, reconciliation, rejected-record analysis, and final Gold queries.

The `data/` directory contains CSV exports of the final analytical outputs, while the `charts/` directory contains the visualizations used to communicate the major findings.

## Limitations

Important limitations include:

* Only **January 2025** was analyzed.
* The data represents **yellow taxis only**.
* Cash tips are not recorded in the TLC dataset, so recorded tip analysis primarily reflects card transactions.
* Yellow taxi activity is highly concentrated in Manhattan and the two major airports.
* Staten Island recorded only **68 pickups** during the month.
* NYC congestion pricing began on January 5, 2025, making simple before-and-after comparisons difficult to interpret causally.

## Generative AI Use

Generative AI tools were used to assist with SQL development, debugging, and review of grammar, clarity, and wording. All generated suggestions and SQL were reviewed by the team, and analytical outputs were verified against actual AWS query results before being included in the final project.

## Course

**MGMT 59900 – Big Data Analytics in the Cloud**
Purdue University Online MSBA
Summer 2026
