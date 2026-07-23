---
layout: post
title: "DataFrames and Spark SQL"
description: "Meet the API you'll actually use day to day — DataFrames, schemas, the Catalyst optimizer, and how to mix DataFrame code with plain SQL in the same job."
date: 2026-07-17
tags: [spark, big-data, tutorial, pyspark, spark-sql]
---

{% include spark-series-nav.html part=4 %}

[Part 3]({{ '/posts/2026/07/16/spark-rdds/' | relative_url }}) covered RDDs — Spark's low-level foundation. Most real-world Spark code doesn't use RDDs directly anymore. It uses **DataFrames**, a higher-level API that's faster, more concise, and easier to reason about. Let's see why.

## What a DataFrame is

A DataFrame is a distributed collection of data organized into named columns with a known schema — conceptually like a table in a relational database, or a pandas DataFrame, except partitioned across a cluster.

The critical difference from an RDD: Spark **knows the structure** of a DataFrame — column names and types — while an RDD is just opaque objects to Spark (in PySpark's case, often opaque Python objects, which carries real overhead). That structure is what lets Spark's **Catalyst optimizer** analyze your query and rewrite it into an efficient execution plan before running anything.

## Creating a DataFrame

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrameIntro").master("local[*]").getOrCreate()

data = [
    ("Alice", "engineering", 95000),
    ("Bob", "sales", 72000),
    ("Carol", "engineering", 108000),
    ("Dave", "sales", 65000),
    ("Eve", "marketing", 81000),
]
columns = ["name", "department", "salary"]

df = spark.createDataFrame(data, columns)
df.show()
```

```
+-----+-----------+------+
| name| department|salary|
+-----+-----------+------+
|Alice|engineering| 95000|
|  Bob|      sales| 72000|
|Carol|engineering|108000|
| Dave|      sales| 65000|
|  Eve|  marketing| 81000|
+-----+-----------+------+
```

Check the inferred schema:

```python
df.printSchema()
```

```
root
 |-- name: string (nullable = true)
 |-- department: string (nullable = true)
 |-- salary: long (nullable = true)
```

## The DataFrame API

Most of the DataFrame API reads like SQL expressed as method chains:

```python
from pyspark.sql.functions import col, avg, desc

# Filter: engineers only
df.filter(col("department") == "engineering").show()

# Select specific columns
df.select("name", "salary").show()

# Sort by salary, descending
df.orderBy(desc("salary")).show()

# Group and aggregate
df.groupBy("department").agg(avg("salary").alias("avg_salary")).show()
```

```
+-----------+----------+
| department|avg_salary|
+-----------+----------+
|engineering|  101500.0|
|      sales|   68500.0|
|  marketing|   81000.0|
+-----------+----------+
```

Note the `import pyspark.sql.functions` module — `col()`, `avg()`, `desc()`, and dozens of other built-in functions live there. Reach for a built-in function before writing a Python UDF; built-ins run inside the JVM without the serialization overhead Python UDFs incur (more on that below).

## Or just write SQL

Every DataFrame can be registered as a temporary view and queried with plain SQL — genuinely the same execution engine underneath, not a translation layer bolted on:

```python
df.createOrReplaceTempView("employees")

result = spark.sql("""
    SELECT department, AVG(salary) AS avg_salary, COUNT(*) AS headcount
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
""")
result.show()
```

```
+-----------+----------+---------+
| department|avg_salary|headcount|
+-----------+----------+---------+
|engineering|  101500.0|        2|
|  marketing|   81000.0|        1|
|      sales|   68500.0|        1|
+-----------+----------+---------+
```

This matters more than it might seem: teams with strong SQL skills but less Python/Scala experience can be immediately productive, and it's often easier to express a complex aggregation in SQL than in method-chain form. Mix and match freely — write the messy parts in SQL, the rest in the DataFrame API, in the same script.

## Word count, revisited

Compare this to the RDD version from Part 2:

```python
from pyspark.sql.functions import explode, split, lower

df = spark.read.text("sample.txt")

word_counts = (
    df.select(explode(split(lower(col("value")), " ")).alias("word"))
      .groupBy("word")
      .count()
      .orderBy(desc("count"))
)
word_counts.show()
```

Same result, and arguably more readable — and, importantly, faster, because Catalyst can optimize this whole pipeline as one unit.

## Why DataFrames are faster: Catalyst

When you call an action on a DataFrame, Spark doesn't just run your code top to bottom. It runs it through **Catalyst**, the query optimizer, which goes through four phases:

1. **Analysis**: resolve column names and types against the schema, catch errors early (e.g., a typo'd column name fails here, before any cluster resources are used).
2. **Logical optimization**: apply rule-based rewrites — predicate pushdown (move filters as early as possible), constant folding, removing redundant projections.
3. **Physical planning**: generate one or more physical execution plans and pick the cheapest based on cost estimates.
4. **Code generation**: compile the final plan into JVM bytecode directly (whole-stage code generation), skipping a lot of the interpretation overhead that would otherwise exist.

You can see the plan Catalyst produced for any DataFrame:

```python
word_counts.explain(True)
```

This prints the parsed, analyzed, optimized, and physical plans — genuinely useful when you're trying to understand why a query is slow. We'll use `.explain()` again in Part 8 when tuning performance.

## A word of caution: Python UDFs

You can write custom functions in Python:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def salary_band(salary):
    if salary >= 90000:
        return "high"
    elif salary >= 70000:
        return "mid"
    return "low"

salary_band_udf = udf(salary_band, StringType())
df.withColumn("band", salary_band_udf(col("salary"))).show()
```

This works, but it's meaningfully slower than built-in functions. A Python UDF forces Spark to serialize each row out of the JVM, hand it to a separate Python process, run your function, and serialize the result back — for every row. Catalyst also can't see inside a Python UDF to optimize around it; it's a black box.

Prefer, in order: built-in `pyspark.sql.functions`, then `pandas_udf` (vectorized, operates on batches via Arrow — much faster than row-at-a-time UDFs), and plain Python UDFs only as a last resort.

## What's next

So far we've only used data created in-memory. In Part 5, we'll read and write real files — CSV, JSON, Parquet — and talk about why Parquet in particular is usually the right default for anything that isn't a one-off script.

---

*Next in the series: [Reading and Writing Data]({{ '/posts/2026/07/18/spark-reading-writing-data/' | relative_url }}).*
