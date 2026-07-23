---
layout: post
title: "RDDs: The Original Building Block"
description: "A proper deep dive into Resilient Distributed Datasets — transformations vs. actions, narrow vs. wide dependencies, lazy evaluation, and why RDDs still matter even though most code today uses DataFrames."
date: 2026-07-16
tags: [spark, big-data, tutorial, pyspark, rdd]
---

{% include spark-series-nav.html part=3 %}

We used RDDs in [Part 2]({{ '/posts/2026/07/15/spark-installation-first-job/' | relative_url }}) without slowing down to explain them properly. Let's fix that — understanding RDDs makes everything later in this series (DataFrames, SQL, streaming) easier, because they're all built on the same underlying execution model.

## What an RDD actually is

An **RDD (Resilient Distributed Dataset)** is an immutable, partitioned collection of records that Spark can operate on in parallel. Unpack that:

- **Immutable**: once created, an RDD never changes. Every transformation produces a *new* RDD rather than modifying the old one.
- **Partitioned**: the data is split into chunks (partitions) distributed across executors. Operations run on each partition independently, in parallel.
- **Resilient**: Spark remembers the sequence of operations used to build each RDD — its **lineage**. If a partition is lost (a node crashes), Spark recomputes just that partition from lineage, rather than needing replicated copies of the data.

```
textFile("sample.txt")   ← lineage remembers: "read this file"
        │
        ▼
   flatMap(split)         ← "then split each line into words"
        │
        ▼
   map(word → (word, 1))  ← "then pair each word with 1"
        │
        ▼
  reduceByKey(+)           ← "then sum by key"
        │
        ▼
   [ partition lost on   ← Spark just replays the lineage
     one executor? ]        for that partition — no replicas needed
```

You can create an RDD two main ways:

```python
# From an existing collection in your driver program
numbers = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# From external storage
lines = spark.sparkContext.textFile("sample.txt")
```

## Transformations vs. actions

This distinction is the single most important thing to understand about Spark's execution model.

**Transformations** build a new RDD from an existing one, and are **lazy** — Spark doesn't compute anything, it just records "here's how to derive this RDD from that one." Examples: `map`, `filter`, `flatMap`, `reduceByKey`, `distinct`, `union`.

**Actions** trigger actual computation and either return a result to the driver or write output. Examples: `collect`, `count`, `take`, `reduce`, `saveAsTextFile`, `foreach`.

```python
rdd = spark.sparkContext.parallelize(range(1, 11))

# All lazy — nothing executes yet
evens = rdd.filter(lambda x: x % 2 == 0)
squared = evens.map(lambda x: x * x)

# This action triggers the entire chain to actually run
result = squared.collect()
print(result)  # [4, 16, 36, 64, 100]
```

Why be lazy at all? Two big reasons:

1. **Optimization.** By waiting until an action is called, Spark sees the *whole* chain of transformations before running any of it, and can optimize across steps — e.g., combining a `filter` and a `map` into a single pass over the data instead of two.
2. **Avoiding wasted work.** If you build a long chain of transformations but only ever call `.take(5)`, Spark doesn't need to process the entire dataset — it can stop early once it has 5 results.

## Narrow vs. wide transformations

Not all transformations are equal in cost. This distinction determines whether Spark needs to move data across the network — a **shuffle**, the single most expensive thing that happens in a Spark job.

**Narrow transformations**: each output partition depends on exactly one input partition. No data movement between executors is needed.

- `map`, `filter`, `flatMap`, `union`

**Wide transformations**: output partitions depend on *multiple* input partitions, potentially spread across different machines. Data has to be shuffled across the network to group it correctly.

- `reduceByKey`, `groupByKey`, `join`, `distinct`, `repartition`

```python
pairs = spark.sparkContext.parallelize([("a", 1), ("b", 2), ("a", 3), ("b", 4)])

# Wide transformation: all "a" records must land on the same partition
# to be summed together, regardless of which partition they started in
result = pairs.reduceByKey(lambda x, y: x + y).collect()
print(result)  # [('a', 4), ('b', 6)]
```

Every wide transformation creates a **stage boundary** — we'll see exactly what that means for job execution in Part 7. For now, the practical takeaway: minimize wide transformations, and when you can't avoid them, prefer `reduceByKey` over `groupByKey` (more on why below).

## `reduceByKey` vs. `groupByKey`

Both group values by key, but `reduceByKey` combines values *before* shuffling data across the network, while `groupByKey` shuffles everything first and combines after.

```python
# groupByKey: ships every individual value across the network,
# THEN groups them — wasteful for large datasets
grouped = pairs.groupByKey().mapValues(sum)

# reduceByKey: combines values locally on each partition first,
# so far less data crosses the network
reduced = pairs.reduceByKey(lambda x, y: x + y)
```

For a dataset with millions of records per key, this difference is enormous — `reduceByKey` can be an order of magnitude faster because it does local pre-aggregation (similar in spirit to a "combiner" in classic MapReduce).

## A slightly bigger example: average by key

```python
data = spark.sparkContext.parallelize([
    ("math", 90), ("math", 70), ("science", 85),
    ("science", 95), ("science", 75), ("math", 100),
])

# Map each value to (value, count) so we can sum both in one pass
sums_counts = data.mapValues(lambda v: (v, 1)) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

averages = sums_counts.mapValues(lambda sc: sc[0] / sc[1])

for subject, avg in averages.collect():
    print(f"{subject}: {avg:.1f}")
```

```
math: 86.7
science: 85.0
```

This pattern — mapping to `(value, count)` pairs so a single `reduceByKey` can compute a sum and count together — is a common trick to avoid two separate passes over the data.

## Persisting RDDs in memory

By default, if you use an RDD twice, Spark recomputes it from scratch both times (remember: RDDs aren't stored, just their lineage). If a computation is expensive and reused, tell Spark to keep it around:

```python
expensive_rdd = spark.sparkContext.textFile("big_file.txt") \
    .filter(lambda line: "ERROR" in line)

expensive_rdd.cache()  # equivalent to .persist(StorageLevel.MEMORY_ONLY)

error_count = expensive_rdd.count()          # computes and caches
first_ten = expensive_rdd.take(10)           # reuses cached data, no recompute
```

We'll cover caching strategy — when to use `MEMORY_ONLY` vs `MEMORY_AND_DISK`, and when caching actually hurts — in Part 8.

## So... should you use RDDs?

Honestly, for most day-to-day work in modern Spark: no, not directly. DataFrames (next post) give you the same distributed engine but with a higher-level API, automatic optimization via Catalyst, and much better performance out of the box, because Spark understands the *structure* of your data rather than treating it as opaque Python objects.

But RDDs are the foundation everything else compiles down to, and understanding transformations, actions, laziness, and shuffles here makes DataFrame performance quirks make a lot more sense later. You'll also still reach for RDDs directly for unstructured data or custom partitioning logic that doesn't fit the DataFrame model.

## What's next

In Part 4, we move to DataFrames and Spark SQL — the API you'll actually use for most real work — and see how the same word count job looks with a fraction of the code.

---

*Next in the series: [DataFrames and Spark SQL]({{ '/posts/2026/07/17/spark-dataframes-sql/' | relative_url }}).*
