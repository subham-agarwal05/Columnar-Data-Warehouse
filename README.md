# Columnar Data Warehouse

## Overview
This project builds a columnar data store, generates a full cuboid lattice, loads it into MySQL, and provides menu-driven business queries plus performance comparisons. The flow is: initialize storage, load data, generate cuboids, optionally load cuboids into MySQL, and run analytics or benchmarks.

## High-Level Workflow
1. Initialize storage for the columnar database.
2. Load denormalized fact data into the columnar files.
3. Generate cuboids (full lattice or iceberg) from the columnar store.
4. Load cuboid CSVs into MySQL for query access.
5. Run interactive queries or performance comparisons.

## Key Schemas
- cds_schema.xml: Column store layout and file paths for each column.
- dim_schema.xml: Logical schema and validation rules for each column.

## Core Binaries and What They Do
- db_init / db_init.cpp: Prepares the DB/ column store layout so later steps can write binary columns consistently.
- db_load_data / db_load_data.cpp: Loads the denormalized fact CSV into the column store, validates schema rules, and maintains dictionaries for encoded columns.
- generate_cuboids / generate_cuboids.cpp: Scans the column store and materializes the full cuboid lattice into CSVs for fast rollups.
- iceberg_cuboids / iceberg_cuboids.cpp: Generates only high-value cuboids based on a SUM threshold to cut storage and runtime.
- clean_cuboids / clean_cuboids.cpp: Cleans previous cuboid outputs to keep reruns consistent.
- sql_lattice / sql_lattice.cpp: Generates cuboids directly in MySQL from the raw fact table for comparison against the columnar path.
- load_to_mysql / load_to_mysql.cpp: Creates MySQL tables from cuboid CSVs and bulk-loads their data for SQL querying and BI access.
- load_raw_mysql / load_raw_mysql.cpp: Loads the raw fact table into MySQL as input for sql_lattice.
- query_cuboids / query_cuboids.cpp: Menu-driven business questions over MySQL cuboids; prints tables and writes CSVs in reports/.
- compare_performance / compare_performance.cpp: Times columnar vs MySQL cuboid generation and produces report.html.
- join_tables.py: Builds a denormalized fact table from source tables to feed the pipeline.

## Data and Output Folders
- DB/: Columnar storage files and dictionaries.
- Data/: Source CSV data used for loading.
- Cuboids/: Full lattice CSV outputs.
- IcebergCuboids_SUM_300000.00/: Iceberg lattice CSV outputs.
- reports/: CSV exports from query_cuboids.
- report.html: Performance comparison report.

## How Everything Helps
- Columnar storage speeds up scanning and aggregation for large analytical workloads.
- Cuboid lattices precompute aggregations to answer business queries quickly.
- Iceberg cuboids keep only high-value groups to cut storage and query time.
- MySQL loading enables easy SQL access and integration with BI tools.
- The query menu provides business-focused insights without writing SQL.
- The performance report documents tradeoffs between the custom columnar engine and MySQL.

## Pipeline (How Everything Is Stitched)
1. join_tables.py creates a denormalized fact CSV (Data/).
2. db_init creates the columnar storage files in DB/.
3. db_load_data loads the fact CSV into DB/ with validation and encoding.
4. generate_cuboids or iceberg_cuboids builds Cuboids/ or IcebergCuboids_*/.
5. load_to_mysql creates MySQL tables from cuboid CSVs.
6. query_cuboids answers business questions and exports reports/.
7. compare_performance benchmarks the columnar path against sql_lattice.

## How to Run (Guide)
1. Build binaries:
	- make
2. Initialize the column store:
	- ./db_init
3. Load data into the column store:
	- ./db_load_data
4. Generate cuboids:
	- ./generate_cuboids
	- Or iceberg: ./iceberg_cuboids 300000 SUM
5. Load cuboids into MySQL:
	- ./load_to_mysql
6. Run business queries:
	- ./query_cuboids
7. (Optional) Run benchmarks:
	- ./compare_performance

## Demonstrating Refresh Feature (Demo Tip)
To show the "refresh" feature to your professor without actually modifying the source CSV, you can manually manipulate the **offset** files. This tricks the system into thinking there is new data to process.

### 1. For Full Lattice (`generate_cuboids`)
- **Logic**: It compares the total rows loaded (`DB/.offset`) with the rows already in cuboids (`DB/.cuboid_offset`).
- **How to Demo**:
  1. Run `./generate_cuboids` (creates all cuboids).
  2. Check `DB/.cuboid_offset` (it will match the total rows).
  3. Manually **decrease** the value in `DB/.cuboid_offset` (e.g., set it to 1000).
  4. Run `./generate_cuboids refresh`.
  5. **Observation**: The system will detect "new data" and perform incremental **upserts** into MySQL.

### 2. For Iceberg Cuboids (`iceberg_cuboids`)
- **Logic**: It compares `DB/.offset` with the offset in the iceberg metadata file.
- **How to Demo**:
  1. Run `./iceberg_cuboids 300000 SUM`.
  2. Locate the metadata file: `DB/.iceberg_metadata_SUM_300000.00`.
  3. Open it and **decrease** the first number (the offset).
  4. Run `./iceberg_cuboids 300000 SUM` again.
  5. **Observation**: The system will detect new data, clear the previous iceberg folder, and **regenerate** the cuboids from the full dataset.

## Example Outputs
Query menu (query_cuboids):
```
1.  Show overall sales, transactions, and order value
2.  Which categories contribute most to revenue?
3.  Which brands bring the most revenue?
...
15. Which sales roles perform best within each segment?
```

Sample query result (abbreviated):
```
+------------------+------------+-------+------------+
| customer_segment | sum_amount | count | avg_amount |
+------------------+------------+-------+------------+
| Corporate        | 1254300.25 |  3120 |     401.37 |
| Consumer         |  983210.10 |  2805 |     350.46 |
+------------------+------------+-------+------------+
```

Performance report:
- report.html contains side-by-side timings and cuboid table counts for columnar vs MySQL runs.
