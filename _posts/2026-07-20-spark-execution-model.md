---
layout: post
title: "Jobs, Stages, and Tasks: How Spark Executes a Query"
description: "What actually happens between calling an action and getting a result — the DAG scheduler, how shuffles split work into stages, and how to read the Spark UI like an execution trace."
date: 2026-07-20
tags: [spark, big-data, tutorial, pyspark, performance]
---

{% include spark-series-nav.html part=7 %}

We've written a lot of Spark code across this series without asking what happens *after* you call `.collect()` or `.show()`. Before tuning performance in Part 8, you need this mental model — it's what makes the Spark UI (introduced in Part 2) actually readable.

## The three levels: job, stage, task

Every action you call decomposes into a hierarchy:

```
Action (e.g. .collect())
  └── Job
        └── Stage 1  ─┐
        └── Stage 2   ├── separated by shuffle boundaries
        └── Stage 3  ─┘
              └── Task, Task, Task, ...  (one per partition)
```

- **Job**: triggered by exactly one action. Call three actions in a script, get three jobs.
- **Stage**: a job is split into stages at every point a **shuffle** is required — i.e., at every wide transformation (recall Part 3: `groupByKey`, `join`, `repartition`, and friends). Within a stage, all operations are narrow transformations that can be pipelined together without moving data across the network.
- **Task**: the actual unit of work sent to an executor — one task per partition, per stage. If a stage's input has 200 partitions, that stage runs 200 tasks (subject to how many can run concurrently, based on available executor cores).

## Building the DAG

As you chain transformations, Spark's **DAG scheduler** builds a **Directed Acyclic Graph** — a graph of RDDs/DataFrames connected by their dependencies, with no cycles. Nothing executes until an action forces evaluation (this is the laziness from Part 3, now at the whole-query level).

Take this pipeline:

```python
result = (
    spark.read.parquet("orders.parquet")
        .filter(col("status") == "completed")      # narrow
        .withColumn("total", col("qty") * col("price"))  # narrow
        .groupBy("region")                          # wide — shuffle!
        .agg(spark_sum("total").alias("revenue"))
        .orderBy(desc("revenue"))                    # wide — shuffle!
)
result.collect()
```

The DAG scheduler looks at this and cuts it into stages wherever a shuffle is unavoidable:

```
Stage 1                    Stage 2                  Stage 3
┌─────────────┐  shuffle  ┌─────────────┐ shuffle  ┌──────────┐
│ read parquet│  ───────▶ │ groupBy     │ ───────▶ │ orderBy  │
│ filter      │  (write   │ region, sum │  (write  │ (global  │
│ withColumn  │  shuffle  │ (partial    │  shuffle │  sort)   │
│ (all narrow,│  files)   │  agg per    │  files)  │          │
│  pipelined) │           │  partition) │          │          │
└─────────────┘           └─────────────┘          └──────────┘
   N tasks                    M tasks                 P tasks
 (= input partitions)      (= shuffle partitions,    (= shuffle
                            default 200)              partitions)
```

Everything inside Stage 1 — reading, filtering, adding a column — happens in one pass per partition, with no data movement, because each output record depends only on one input record. That's the payoff of narrow transformations: Spark fuses them into a single pipelined stage rather than running them as separate steps.

The two `groupBy`/`orderBy` operations each force a shuffle: data has to be physically regrouped across the cluster so that, e.g., all rows for `region = "west"` land on the same partition before they can be summed together.

## Reading this in the Spark UI

Run the pipeline above and open the **Jobs** tab at `localhost:4040`. Click into the job, and you'll see exactly this: a DAG visualization with stage boxes, arrows for shuffle dependencies, and a task count per stage.

Click into a stage to see its task table — duration per task, data read/written, and crucially, whether tasks are **skewed**. If 199 tasks finish in 2 seconds and 1 task takes 3 minutes, that's a sign one partition has far more data than the others (data skew), which we'll address directly in Part 8.

A few things worth checking every time you look at a stage:

- **Task count vs. core count**: if you have 8 cores total and a stage has 4 tasks, you're leaving 4 cores idle. Too few partitions underutilizes the cluster.
- **Shuffle read/write size**: large numbers here mean a lot of network and disk I/O — a prime target for optimization.
- **Max vs. median task duration**: a big gap signals skew, one of the most common real-world Spark performance problems.

## Why `spark.sql.shuffle.partitions` matters

The default number of partitions after a shuffle is **200**, controlled by `spark.sql.shuffle.partitions`. This default is a reasonable middle ground, but it's not tuned to your data:

```python
spark.conf.set("spark.sql.shuffle.partitions", 50)
```

- Too many partitions relative to your data size → lots of small tasks, each with scheduling overhead that dwarfs the actual work. You'll see hundreds of tasks finishing in milliseconds.
- Too few partitions relative to your data size → each task handles too much data, runs for a long time, and you don't get enough parallelism to use the cluster fully.

A common rule of thumb: aim for partitions in the range of ~100–200MB of data each. We'll apply this concretely in Part 8.

## `.explain()` shows you the same thing, as text

For DataFrame/SQL queries, `.explain(mode="formatted")` prints the physical plan Catalyst chose — which is, in effect, a text description of the stages you'd see in the UI:

```python
result.explain(mode="formatted")
```

This is often faster to check than opening the UI, especially while iterating on a query in a notebook — get in the habit of glancing at it whenever a query's performance surprises you.

## What's next

With the execution model in hand, Part 8 gets practical: caching strategy, partitioning your data sensibly, diagnosing and fixing data skew, and reading executor memory metrics to catch problems before they cause an out-of-memory failure.

---

*Next in the series: [Performance Tuning]({{ '/posts/2026/07/21/spark-performance-tuning/' | relative_url }}).*
