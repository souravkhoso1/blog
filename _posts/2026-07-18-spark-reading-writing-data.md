---
layout: post
title: "Reading and Writing Data: Files, Formats, and Databases"
description: "CSV, JSON, Parquet, and JDBC in Spark — how to read and write each, why file format choice matters more than people expect, and partitioning your output sensibly."
date: 2026-07-18
tags: [spark, big-data, tutorial, pyspark, parquet]
---

{% include spark-series-nav.html part=5 %}

Every Spark job starts by reading data and (usually) ends by writing it somewhere. [Part 4]({{ '/posts/2026/07/17/spark-dataframes-sql/' | relative_url }}) used in-memory data to keep focus on the API; this post covers the formats and sources you'll actually hit in practice.

## The unified reader/writer API

Spark exposes one consistent interface for reading, `spark.read`, and writing, `df.write`, regardless of format:

```python
# Reading
df = spark.read.format("csv").option("header", True).load("path/to/file.csv")

# Equivalent, shorter form for common formats
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)

# Writing
df.write.format("parquet").mode("overwrite").save("path/to/output/")
```

The `mode` argument controls what happens if the output path already has data: `"overwrite"` replaces it, `"append"` adds to it, `"error"` (the default) fails, and `"ignore"` silently does nothing.

## CSV: convenient, but slow and fragile

```python
df = spark.read.csv(
    "employees.csv",
    header=True,
    inferSchema=True,
)
```

`inferSchema=True` makes Spark do a preliminary pass over the data to guess column types — convenient for exploration, but it costs an extra read of the data and can guess wrong (e.g., a column of mostly integers with one stray text value gets typed as string). For anything beyond ad hoc scripts, define the schema explicitly:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
])

df = spark.read.csv("employees.csv", header=True, schema=schema)
```

This skips the inference pass entirely and fails fast if the data doesn't match what you expect — much better than silently mis-typing a column.

## JSON: flexible, also slow

```python
df = spark.read.json("events.json")
df.printSchema()  # Spark infers nested structure automatically
```

Spark handles nested JSON natively — nested objects become `struct` columns, arrays become `array` columns, and you can drill into them with dot notation or `explode()`:

```python
from pyspark.sql.functions import explode

# events.json has a field "tags": ["a", "b", "c"] per record
df.select("event_id", explode("tags").alias("tag")).show()
```

Both CSV and JSON are **row-based, text formats**: every field is re-parsed from text on every read, there's no compression by default, and reading one column still means scanning every byte of every row. Fine for small files or data exchange with other systems. Not what you want for anything at scale.

## Parquet: the default for a reason

**Parquet** is a columnar, binary, compressed file format, and it should be your default for anything beyond a one-off script or a hand-off to a non-Spark tool.

```python
df.write.mode("overwrite").parquet("output/employees_parquet")

df2 = spark.read.parquet("output/employees_parquet")
```

Why it matters:

```
Row-based (CSV/JSON)              Columnar (Parquet)
┌────┬──────┬────────┐            ┌────┬────┬────┐
│name│ dept │ salary │            │name│dept│salary
├────┼──────┼────────┤            ├────┼────┼────┤
│Alice│ eng │ 95000  │            │Alice│ eng│95000
│Bob  │ sales│ 72000 │   ──────▶  │Bob  │... │72000
│Carol│ eng │ 108000 │            │Carol│    │108000
└────┴──────┴────────┘            └────┴────┴────┘
  read row-by-row,                 read column-by-column,
  every field every time           skip columns you don't need
```

- **Columnar storage**: reading just `salary` for an aggregation doesn't require touching the `name` or `department` bytes at all. For wide tables where a query only touches a few columns, this is a massive I/O reduction.
- **Compression**: columnar data compresses far better than row data, because values within a column tend to be similar (same type, often repeated or close in range). Parquet files are routinely a fraction of the size of the equivalent CSV.
- **Schema embedded in the file**: no inference pass needed, no ambiguity, no separate schema file to keep in sync.
- **Predicate and column pushdown**: Spark can skip entire row groups within a Parquet file if it knows (from file-level statistics) that no rows match a filter — without reading them at all.

Other columnar formats exist (ORC, common in the Hive ecosystem) but Parquet is the most broadly supported default across the Spark, pandas, and cloud-warehouse ecosystem.

## Reading from a database with JDBC

```python
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "employees") \
    .option("user", "postgres") \
    .option("password", "secret") \
    .load()
```

By default this reads through a *single* database connection — no parallelism, and potentially a bottleneck for large tables. To parallelize, tell Spark how to split the read across a numeric column:

```python
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "employees") \
    .option("partitionColumn", "employee_id") \
    .option("lowerBound", 1) \
    .option("upperBound", 1000000) \
    .option("numPartitions", 10) \
    .option("user", "postgres") \
    .option("password", "secret") \
    .load()
```

This issues 10 parallel queries, each fetching a range of `employee_id`, and assembles the results into 10 DataFrame partitions. Never hardcode credentials like this in a real script — use a secrets manager or environment variables instead.

## Controlling output layout: partitioning

When writing large datasets, `partitionBy` splits output into a directory structure by column value — hugely useful for downstream tools that only need to read a slice of the data:

```python
df.write.mode("overwrite") \
    .partitionBy("department") \
    .parquet("output/employees_by_dept")
```

```
output/employees_by_dept/
├── department=engineering/
│   └── part-00000....parquet
├── department=sales/
│   └── part-00000....parquet
└── department=marketing/
    └── part-00000....parquet
```

A later read filtering on `department == "sales"` can skip the other directories entirely — this is called **partition pruning**, and it's one of the cheapest performance wins available. Pick partition columns you'll actually filter on often (date is the classic choice for time-series data), and avoid high-cardinality columns (like a user ID) — that just produces thousands of tiny directories, which hurts more than it helps.

## What's next

Now that data is flowing in and out cleanly, Part 6 covers combining and aggregating it — joins, group-bys, and window functions, plus the different join strategies Spark chooses between and why that choice matters for performance.

---

*Next in the series: [Joins, Aggregations, and Window Functions]({{ '/posts/2026/07/19/spark-joins-aggregations-window-functions/' | relative_url }}).*
