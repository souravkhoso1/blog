---
layout: post
title: "Apache Spark 101: What It Is and Why It Exists"
description: "Kicking off a 10-part Spark tutorial series — what problem Spark solves, how it compares to Hadoop MapReduce, and the core architecture you need in your head before writing a line of code."
date: 2026-07-14
tags: [spark, big-data, tutorial, pyspark]
---

{% include spark-series-nav.html part=1 %}

This is the first post in a 10-part series on Apache Spark. The goal isn't to memorize API calls — it's to build a working mental model of how Spark thinks about data, then get hands-on with code in every session. By the end you'll be comfortable with RDDs, DataFrames, Spark SQL, streaming, and running real jobs on a cluster.

Let's start with the question that matters most: what problem does Spark actually solve?

## The problem: data too big for one machine

Say you have 500GB of log files and you need to count how many requests came from each country. On a single machine, this is slow but doable — read the file, parse each line, keep a running count. Give it enough time and enough RAM, it finishes.

Now say you have 50TB. One machine won't have the disk to hold it, let alone the RAM to process it. You need many machines working together, splitting the data and the computation between them.

That's *distributed computing*, and it's a genuinely hard problem. If you split the work across 100 machines, you now have to worry about: how do you divide the data fairly? What happens when one machine dies mid-job? How do results from 100 machines get combined? How do you avoid one slow machine holding up all 99 others?

## Before Spark: Hadoop MapReduce

The first popular answer to this was Hadoop MapReduce, released by Google (as a paper) in 2004 and open-sourced via Apache Hadoop shortly after. MapReduce broke every job into two phases:

- **Map**: transform each piece of data independently (e.g., emit `(country, 1)` for each log line)
- **Reduce**: combine results that share a key (e.g., sum up the `1`s per country)

MapReduce worked, and it powered enormous systems at Yahoo, Facebook, and elsewhere. But it had a defining weakness: **every intermediate result was written to disk.** Map output hit disk. Reduce read it back from disk. If you chained multiple MapReduce jobs together — common in real pipelines — you paid that disk round-trip cost again and again.

For iterative workloads (think: machine learning algorithms that loop over the same dataset hundreds of times, or interactive queries where an analyst runs one query after another against the same data), this was brutal. Disk I/O dominated the runtime.

## Spark's core idea: keep data in memory

Spark, started at UC Berkeley's AMPLab in 2009 and open-sourced in 2010, made one central bet: **memory is much faster than disk, so keep data there between operations whenever possible.**

The abstraction that makes this safe and efficient is the **RDD — Resilient Distributed Dataset**. We'll go deep on RDDs in Part 3, but the short version:

- **Resilient**: if a machine (or "node") holding part of the data dies, Spark can recompute just that piece, because it remembers *how* the data was derived, not just the data itself.
- **Distributed**: the dataset is split into partitions spread across the cluster.
- **Dataset**: it's just a collection of records — could be lines of text, rows, tuples, whatever.

Because Spark tracks the lineage of *how* each RDD was built from the last, it doesn't need to replicate data for fault tolerance the way some systems do. It can just redo the computation.

The practical effect: workloads that touch the same data repeatedly — iterative ML training, interactive exploration, multi-stage pipelines — can run 10-100x faster than equivalent MapReduce jobs, because Spark avoids re-reading and re-writing to disk at every step.

## Spark's architecture, in one picture

Every Spark application has the same basic shape:

```
                 ┌─────────────────────┐
                 │   Driver Program     │  ← your code runs here
                 │  (SparkContext /     │
                 │   SparkSession)      │
                 └──────────┬───────────┘
                             │
                    negotiates resources
                             │
                 ┌───────────▼───────────┐
                 │    Cluster Manager     │  (Standalone, YARN,
                 │                        │   Kubernetes, Mesos)
                 └───────────┬───────────┘
                             │
           ┌─────────────────┼─────────────────┐
           ▼                 ▼                 ▼
     ┌───────────┐    ┌───────────┐     ┌───────────┐
     │  Executor  │    │  Executor  │     │  Executor  │
     │ (worker 1) │    │ (worker 2) │     │ (worker 3) │
     └───────────┘    └───────────┘     └───────────┘
```

- **Driver**: the process running your `main()` — it builds up the computation as a graph of transformations, then asks the cluster manager for resources to run it.
- **Cluster manager**: hands out worker resources. Could be Spark's own built-in standalone manager, YARN, Kubernetes, or Mesos.
- **Executors**: processes on worker nodes that actually run tasks and hold data in memory or on disk for your application.

This same architecture works whether you're running on your laptop with one process pretending to be a whole cluster, or across a thousand machines in a data center. That's part of why Spark code you write locally usually just works when you point it at a real cluster later.

## Why Spark, not just "more Hadoop"

A few things pushed Spark to become the dominant engine in this space:

- **Unified engine.** Before Spark, you often needed separate systems for batch processing (MapReduce), SQL queries (Hive), streaming (Storm), and machine learning (Mahout). Spark does all of these under one API and one execution engine — that's Spark SQL, Structured Streaming, and MLlib, which we'll cover in later sessions.
- **Language flexibility.** APIs in Scala, Java, Python (PySpark), and R. Most people learning Spark today start with PySpark, which is what this series will mostly use.
- **Rich, high-level APIs.** DataFrames (Part 4) let you express computation declaratively, closer to SQL or pandas than to raw MapReduce-style code.
- **A real optimizer.** Spark SQL's Catalyst optimizer rewrites your query into an efficient physical execution plan automatically — you write *what* you want, Spark figures out a fast *how*.

## What's next

In Part 2, we'll install Spark, launch `pyspark` interactively, and run your first real job — counting words in a text file, the "hello world" of distributed computing, plus a look at the Spark UI so you can actually see what your job is doing.

---

*Next in the series: [Installing Spark and Your First Job]({{ '/posts/2026/07/15/spark-installation-first-job/' | relative_url }}).*
