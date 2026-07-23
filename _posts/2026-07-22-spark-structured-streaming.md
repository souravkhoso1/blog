---
layout: post
title: "Structured Streaming: Real-Time Data with Spark"
description: "How Spark's micro-batch streaming engine works, reading from sockets and Kafka, output modes, windowed aggregations over event time, and handling late data with watermarks."
date: 2026-07-22
tags: [spark, big-data, tutorial, pyspark, streaming]
---

{% include spark-series-nav.html part=9 %}

Everything through [Part 8]({{ '/posts/2026/07/21/spark-performance-tuning/' | relative_url }}) processed **batch** data — a fixed dataset that exists in full before your job starts. Structured Streaming lets you run that same DataFrame API against data that's continuously arriving, without learning a new programming model.

## The core idea: an unbounded table

Structured Streaming's mental model: treat a stream as a table that new rows keep getting appended to. You write a query against this "unbounded table" exactly like you'd query a static one — Spark handles running it incrementally as new data shows up.

```
Batch DataFrame                    Streaming DataFrame
┌─────────────┐                    ┌─────────────┐
│ fixed rows, │                    │ rows keep    │ ◀── new data
│ known size  │                    │ arriving...  │     keeps appending
└─────────────┘                    └─────────────┘
      │                                   │
   df.groupBy(...)                   df.groupBy(...)
   .count()                          .count()
      │                                   │
      ▼                                   ▼
  one result,                     result updates
  computed once                   incrementally, on
                                   each new micro-batch
```

Under the hood, Spark doesn't process data byte-by-byte as it arrives — it collects new data into small **micro-batches** and runs your query against each batch incrementally, merging results into the running output. This is different from true row-at-a-time streaming systems (like Flink's native streaming mode), but in exchange, you get the same reliable, well-optimized batch engine you've been using all series, plus exactly-once processing guarantees.

## A minimal streaming query

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower

spark = SparkSession.builder.appName("StreamWordCount").master("local[*]").getOrCreate()

# Source: a socket (for local testing — run `nc -lk 9999` in another terminal)
lines = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

words = lines.select(explode(split(lower(lines.value), " ")).alias("word"))
word_counts = words.groupBy("word").count()

query = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
```

Run `nc -lk 9999` in a terminal, type some words, and watch counts update in the console as you type. The only new pieces compared to batch code: `readStream` instead of `read`, and `writeStream`/`.start()` instead of `write`/`.save()` — everything in between is the exact same DataFrame API from Parts 4-6.

## Output modes

Streaming queries need to say how results should be written each time new data arrives:

- **`complete`**: rewrite the entire result table every batch. Only works for aggregations, where the full result is small enough to redo each time (as in the word count example above).
- **`append`**: only write new rows since the last batch. Works for queries without aggregation, or aggregations with a watermark (below) so Spark knows a row is finalized and won't change.
- **`update`**: write only rows that changed since the last batch — a middle ground, useful for dashboards that want incremental updates without resending the whole table.

## Reading from Kafka

Sockets are for local testing; real pipelines almost always read from Kafka:

```python
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

# Kafka messages arrive as raw bytes — cast and parse as needed
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

schema = StructType() \
    .add("order_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("region", StringType())

orders = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")
```

## Windowed aggregations over event time

A common streaming pattern: aggregate over a rolling time window, based on *when the event actually happened* (event time, from a timestamp field in the data) rather than when Spark happened to process it (processing time) — important because network delays and retries mean data doesn't always arrive in perfect order.

```python
from pyspark.sql.functions import window, sum as spark_sum

windowed_totals = orders \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window("event_time", "5 minutes"), "region") \
    .agg(spark_sum("amount").alias("total_sales"))
```

`window("event_time", "5 minutes")` buckets events into non-overlapping 5-minute windows based on their own timestamp, regardless of processing order.

## Watermarks: handling late data without waiting forever

`.withWatermark("event_time", "10 minutes")` tells Spark: "once we've seen an event with timestamp T, assume we won't see any more events more than 10 minutes older than T." This lets Spark know when a window is safe to finalize and drop from memory — without a watermark, Spark would have to keep *all* window state around forever, since a late-arriving event could always update an old window.

```
Event time  ──────────────────────────────────────▶

Window [10:00–10:05]   ████████████░░░  still open, accepting late data
                                    ▲
                          watermark = max seen time − 10 min
                          once watermark passes 10:05, window closes
                          and any data older than that is dropped
```

There's a real tradeoff here: a longer watermark tolerates more out-of-order data correctly, but keeps more state in memory for longer and delays final results. A shorter watermark finalizes faster but may drop genuinely late data. Pick the watermark based on how late your data realistically arrives in practice — not arbitrarily.

## What's next

We've now covered the whole core engine: batch, SQL, and streaming. The last post in this series steps back from code and covers running Spark for real — cluster managers, deployment modes, and the operational basics you need before running any of this in production.

---

*Next in the series: [Deploying Spark to Production]({{ '/posts/2026/07/23/spark-deploying-production/' | relative_url }}).*
