---
layout: post
title: "Joins, Aggregations, and Window Functions"
description: "Join strategies Spark chooses between (and how to influence that choice), grouped aggregations, and window functions for running totals, rankings, and row-relative comparisons."
date: 2026-07-19
tags: [spark, big-data, tutorial, pyspark, spark-sql]
---

{% include spark-series-nav.html part=6 %}

With data flowing in and out cleanly ([Part 5]({{ '/posts/2026/07/18/spark-reading-writing-data/' | relative_url }})), this post covers combining and summarizing it: joins, aggregations, and window functions — the three tools behind most real analytical queries.

## Joins

```python
employees = spark.createDataFrame([
    (1, "Alice", "eng"), (2, "Bob", "sales"), (3, "Carol", "eng"),
], ["id", "name", "dept"])

departments = spark.createDataFrame([
    ("eng", "Engineering", "Building A"),
    ("sales", "Sales", "Building B"),
    ("marketing", "Marketing", "Building C"),
], ["dept", "full_name", "location"])

result = employees.join(departments, on="dept", how="inner")
result.show()
```

```
+-----+---+-----+-----------+----------+
| dept| id| name|  full_name|  location|
+-----+---+-----+-----------+----------+
|  eng|  1|Alice|Engineering|Building A|
|  eng|  3|Carol|Engineering|Building A|
|sales|  2|  Bob|      Sales|Building B|
+-----+---+-----+-----------+----------+
```

Standard join types are all supported via `how`: `inner` (default), `left` / `left_outer`, `right` / `right_outer`, `full` / `full_outer`, `left_semi` (rows from the left with a match, but only left's columns), and `left_anti` (rows from the left with *no* match — useful for finding orphaned records).

## How Spark actually executes a join

Under the hood, Spark picks between a few physical strategies, and the choice has a big performance impact.

**Shuffle (sort-merge) join**: when both sides are large, Spark shuffles both DataFrames so that matching keys land on the same partition, then joins locally on each partition. This is correct for any size of data, but it's expensive — every row of both tables crosses the network.

```
   employees (large)              departments (large)
        │                               │
    shuffle by dept                shuffle by dept
        │                               │
        └───────────┬───────────────────┘
                     ▼
        matching keys co-located,
          joined locally per partition
```

**Broadcast hash join**: when one side is small enough to fit comfortably in executor memory, Spark instead sends a full copy of the small DataFrame to *every* executor, and joins locally against the large DataFrame's existing partitions — no shuffle of the large side needed at all.

```
                 departments (small)
                        │
        ┌───────────────┼───────────────┐
        ▼                ▼               ▼
  copied in full   copied in full   copied in full
  to executor 1     to executor 2    to executor 3
        │                │               │
   joined locally against employees' existing partitions
   (no shuffle of the large side)
```

Spark decides automatically based on the estimated size of each side (`spark.sql.autoBroadcastJoinThreshold`, 10MB by default), but you can hint it explicitly when you know better than Spark's size estimate:

```python
from pyspark.sql.functions import broadcast

result = employees.join(broadcast(departments), on="dept", how="inner")
```

For joins against a small lookup/dimension table (departments, country codes, currency rates), forcing a broadcast join is one of the highest-leverage performance tweaks available — it can turn a slow shuffle join into a fast, shuffle-free one. Check `.explain()` to confirm which strategy Spark actually used.

## Aggregations

Simple `groupBy` aggregations were covered briefly in Part 4. You can compute several aggregates at once, and combine multiple conditions:

```python
from pyspark.sql.functions import count, avg, min, max, sum as spark_sum

employees_df.groupBy("dept").agg(
    count("*").alias("headcount"),
    avg("salary").alias("avg_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    spark_sum("salary").alias("total_payroll"),
).show()
```

`pivot` reshapes grouped data into a wide table — useful for turning row values into columns, e.g. a sales-by-quarter report:

```python
sales.groupBy("region").pivot("quarter").agg(spark_sum("revenue")).show()
```

```
+-------+-----+-----+-----+-----+
| region|   Q1|   Q2|   Q3|   Q4|
+-------+-----+-----+-----+-----+
|  North|12000|15000|13500|17000|
|  South| 9800|11200|10500|12800|
+-------+-----+-----+-----+-----+
```

## Window functions

`groupBy` collapses rows into one row per group. **Window functions** compute a value *per row*, relative to a set of related rows ("the window"), without losing row-level detail — running totals, rankings, row-over-row comparisons.

```python
from pyspark.sql import Window
from pyspark.sql.functions import rank, row_number, lag, sum as spark_sum

window_spec = Window.partitionBy("dept").orderBy(desc("salary"))

ranked = employees_df.withColumn("rank", rank().over(window_spec))
ranked.show()
```

```
+-----+-----------+------+----+
| name|       dept|salary|rank|
+-----+-----------+------+----+
|Carol|engineering|108000|   1|
|Alice|engineering| 95000|   2|
|  Eve|  marketing| 81000|   1|
|  Bob|      sales| 72000|   1|
| Dave|      sales| 65000|   2|
+-----+-----------+------+----+
```

A `Window` spec has three parts: `partitionBy` (like `groupBy`, but rows stay separate), `orderBy` (ordering within each partition), and an optional frame (which rows around the current one to include — defaults to "all rows in the partition" for ranking functions, but can be narrowed for rolling calculations).

Running total, a classic window function use case:

```python
running_total_window = Window.partitionBy("dept").orderBy("employee_id") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

employees_df.withColumn(
    "running_payroll", spark_sum("salary").over(running_total_window)
).show()
```

Comparing a row to the previous one with `lag` (or the next one with `lead`) — useful for month-over-month change, detecting gaps, and similar:

```python
sales_by_month.withColumn(
    "prev_month_revenue", lag("revenue", 1).over(Window.partitionBy("region").orderBy("month"))
).show()
```

Window functions run entirely within Spark SQL's engine — no shuffling data out to Python — so they're fast, and usually a better choice than trying to hand-roll the same logic with `groupBy` and self-joins.

## What's next

We've been running everything in local mode and reasoning about correctness. Part 7 opens up the Spark UI properly and walks through exactly how a query becomes jobs, stages, and tasks — the mental model you need before performance tuning makes sense.

---

*Next in the series: [Jobs, Stages, and Tasks]({{ '/posts/2026/07/20/spark-execution-model/' | relative_url }}).*
