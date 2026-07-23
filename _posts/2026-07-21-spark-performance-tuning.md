---
layout: post
title: "Performance Tuning: Partitioning, Caching, and Shuffles"
description: "A practical tuning checklist — when and how to cache, fixing data skew, choosing partition counts, and reading executor memory metrics before they turn into out-of-memory failures."
date: 2026-07-21
tags: [spark, big-data, tutorial, pyspark, performance]
---

{% include spark-series-nav.html part=8 %}

[Part 7]({{ '/posts/2026/07/20/spark-execution-model/' | relative_url }}) built the mental model — jobs, stages, tasks, shuffles. Now let's use it to actually make slow jobs fast.

## Caching: when it helps, when it hurts

Caching keeps a DataFrame's computed result in memory (or disk) across multiple actions, avoiding recomputation from source:

```python
filtered = spark.read.parquet("events.parquet").filter(col("status") == "active")
filtered.cache()

count1 = filtered.count()          # computes and caches
top10 = filtered.orderBy(desc("ts")).limit(10).collect()  # reuses cache
```

Cache when a DataFrame is:
- **Reused multiple times** — if you only touch a DataFrame once, caching just adds overhead for no benefit.
- **Expensive to recompute** — the result of a costly join or aggregation, not a cheap filter on already-cached data.

Storage levels, via `.persist(StorageLevel...)`:

```python
from pyspark.storagelevel import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)      # fastest, lost if it doesn't fit
df.persist(StorageLevel.MEMORY_AND_DISK)  # spills to disk if it doesn't fit — the safe default
df.persist(StorageLevel.DISK_ONLY)        # for data too big for memory, still avoids recompute
```

`MEMORY_AND_DISK` is the sensible default for most cases — `MEMORY_ONLY` risks silently dropping cached partitions (forcing recomputation later) if memory pressure gets high.

Always release a cache you no longer need:

```python
filtered.unpersist()
```

Check the **Storage** tab in the Spark UI to see exactly what's cached, how much memory it's using, and whether any of it has spilled to disk — a quick way to catch a cache that isn't earning its keep.

## Diagnosing data skew

Skew is when one or a few partitions hold dramatically more data than the rest — usually because a `groupBy` or `join` key is unevenly distributed (a handful of customer IDs with 90% of all orders, for example). You'll see it in the Spark UI as one or two tasks taking far longer than the rest of a stage.

Check for skew directly:

```python
from pyspark.sql.functions import spark_partition_id, count

df.groupBy(spark_partition_id()).agg(count("*").alias("rows")).orderBy(desc("rows")).show()
```

If one partition has 50x the rows of the median, that's your skew.

```
Balanced partitions               Skewed partitions
┌───┬───┬───┬───┬───┐            ┌───┬───┬───┬───┬────────┐
│ 20│ 22│ 19│ 21│ 20│            │ 12│ 10│  8│ 11│   200   │
└───┴───┴───┴───┴───┘            └───┴───┴───┴───┴────────┘
 all tasks finish                 4 tasks finish fast,
 around the same time             1 task drags the whole stage out
```

Common fixes:

- **Salting**: append a random suffix to a skewed key to spread it across more partitions, then aggregate in two steps (once on the salted key, then again on the real key). Manual, but effective for extreme skew.
- **Adaptive Query Execution (AQE)**: since Spark 3.0, enabled by default in recent versions, AQE detects skewed partitions at runtime and automatically splits them into smaller sub-partitions. Confirm it's on:

```python
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
```

AQE also dynamically coalesces small shuffle partitions and can switch a sort-merge join to a broadcast join at runtime if it learns one side turned out to be small after filtering — it's genuinely one of the best "free" performance wins in modern Spark, and worth confirming is enabled before doing manual tuning.

## Choosing partition counts

From Part 7: too many partitions means task-scheduling overhead dominates; too few means underused parallelism and long individual tasks.

```python
# Check current partition count
df.rdd.getNumPartitions()

# Reduce partitions without a full shuffle (only merges adjacent partitions —
# use when reducing count, e.g. before writing fewer, larger output files)
df.coalesce(10)

# Change partition count with a full shuffle (use when increasing count,
# or when you need genuinely even redistribution, not just merging)
df.repartition(50)

# Repartition by a column — useful before a groupBy/join on that column,
# or to control output file layout by key
df.repartition(20, "region")
```

`coalesce` is cheaper because it avoids a full shuffle — it just combines existing partitions — but it can't increase partition count or fix uneven distribution. `repartition` shuffles everything to guarantee even-sized output partitions, at the cost of a shuffle.

## Reading executor memory metrics

The **Executors** tab in the Spark UI shows, per executor: memory used, memory available, disk spill, and task/shuffle failures. Spill — data that didn't fit in memory during a shuffle or aggregation and had to write to disk — is a strong signal you're memory-constrained:

```
Storage Memory    ████████████████████░░░░  used / available
Shuffle Read      1.2 GB
Shuffle Write     980 MB
Spill (Memory)    340 MB   ← data spilled to disk mid-computation
Spill (Disk)      120 MB
```

Some spill under heavy load is normal; consistently large spill across most executors usually means either the executor memory allocation is too small for the workload, or a partition count that's too low (each task handling more data than it should).

Executor memory can be set at submit time:

```bash
spark-submit --executor-memory 4g --executor-cores 4 --num-executors 10 my_job.py
```

## A basic tuning checklist

When a job is slow, work through these roughly in order — they're ordered by how often they're the actual cause, in practice:

1. Check the Spark UI: which stage is slow, and is it skewed (uneven task durations)?
2. Confirm AQE is enabled.
3. Check partition count relative to data size and available cores.
4. Look for `groupByKey` where `reduceByKey`-style pre-aggregation would work (Part 3).
5. Check whether a join could be a broadcast join instead of a shuffle join (Part 6).
6. Look for repeated computation on the same DataFrame that should be cached.
7. Check executor memory metrics for spill.
8. Only then, consider more advanced options: custom partitioners, bucketing, or increasing cluster size.

Bigger clusters and more memory are a lever, but they're usually the last one to pull — most real-world slowness comes from skew, an avoidable shuffle, or a missing cache, all of which more hardware doesn't fix.

## What's next

Everything so far has processed data that already exists at rest. Part 9 introduces Structured Streaming — running the same DataFrame API you already know against data that's continuously arriving.

---

*Next in the series: [Structured Streaming]({{ '/posts/2026/07/22/spark-structured-streaming/' | relative_url }}).*
