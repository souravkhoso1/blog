---
layout: post
title: "Installing Spark and Your First Job"
description: "Get PySpark running locally, launch a Spark shell, run the classic word count job, and take a tour of the Spark UI to see what actually happened."
date: 2026-07-15
tags: [spark, big-data, tutorial, pyspark]
---

{% include spark-series-nav.html part=2 %}

In [Part 1]({{ '/posts/2026/07/14/spark-101-introduction/' | relative_url }}) we covered why Spark exists and its high-level architecture. Now let's get it running and write real code.

## Installing PySpark

The fastest path for local development is the `pyspark` pip package, which bundles everything you need — no separate Java install of Spark itself required, though you do need a JDK.

```bash
# Java is a prerequisite — check you have JDK 8, 11, or 17
java -version

# Create a virtual environment (recommended)
python3 -m venv spark-env
source spark-env/bin/activate

# Install PySpark
pip install pyspark
```

Verify it works:

```bash
pyspark --version
```

You should see the Spark version banner print out. That's it — no cluster, no config files, just a working local Spark install.

## Launching an interactive shell

Run:

```bash
pyspark
```

This drops you into a Python REPL with a `SparkSession` already created for you, bound to the variable `spark`. Try it:

```python
>>> spark
<pyspark.sql.session.SparkSession object at 0x...>

>>> spark.version
'3.5.1'
```

`SparkSession` is your entry point to everything — DataFrames, SQL, configuration. In scripts (as opposed to the interactive shell), you create it yourself:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyFirstApp") \
    .master("local[*]") \
    .getOrCreate()
```

`master("local[*]")` tells Spark to run locally using all available CPU cores as "workers" — no cluster needed. In Part 10 we'll swap this out for a real cluster manager without changing anything else about the code.

## Your first job: word count

Word count is the "hello world" of distributed computing for good reason — it touches reading data, transforming it, aggregating by key, and writing output, which covers most of what a real job does.

Create a text file to work with:

```bash
cat > sample.txt << 'EOF'
Spark makes big data processing fast
Spark is fast because it uses memory
Fast processing means faster insights
EOF
```

Now, in the `pyspark` shell:

```python
# Read the file into an RDD, one element per line
lines = spark.sparkContext.textFile("sample.txt")

# Split each line into words, flatten into one big list of words
words = lines.flatMap(lambda line: line.split(" "))

# Turn each word into a (word, 1) pair
pairs = words.map(lambda word: (word.lower(), 1))

# Sum counts per word
counts = pairs.reduceByKey(lambda a, b: a + b)

# Pull results back to the driver and print
for word, count in counts.collect():
    print(f"{word}: {count}")
```

Output (order may vary):

```
spark: 2
makes: 1
big: 1
data: 1
processing: 2
fast: 3
is: 1
because: 1
it: 1
uses: 1
memory: 1
means: 1
faster: 1
insights: 1
```

Nothing here ran until `.collect()` was called. That's Spark's **lazy evaluation** model: `textFile`, `flatMap`, `map`, and `reduceByKey` just build up a plan — a graph of transformations — without touching data. Only an **action** like `.collect()`, `.count()`, or `.take()` actually triggers execution. We'll dig into why this matters (and how it enables optimization) in Part 3.

## Running it as a script

Interactive shells are great for exploration, but real jobs live in `.py` files, submitted with `spark-submit`:

```python
# wordcount.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WordCount") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
lines = sc.textFile("sample.txt")
counts = (
    lines.flatMap(lambda line: line.split(" "))
         .map(lambda word: (word.lower(), 1))
         .reduceByKey(lambda a, b: a + b)
)

for word, count in counts.collect():
    print(f"{word}: {count}")

spark.stop()
```

Run it with:

```bash
spark-submit wordcount.py
```

`spark-submit` is the standard way to launch any Spark application, whether locally or on a real cluster — you'll use it (with different flags) all the way through this series.

Always call `spark.stop()` at the end of a script to release resources cleanly. In the interactive shell this happens automatically when you exit.

## Touring the Spark UI

While a job is running (or briefly after, in local mode), Spark exposes a web UI at **http://localhost:4040** with live details about what's happening. This is one of the most useful debugging tools you'll use in this series, so open it now while a job runs — add a `time.sleep(60)` before `spark.stop()` if the job finishes too fast to see.

Key tabs:

- **Jobs**: every action (`.collect()`, `.count()`, etc.) you trigger shows up here as a job, broken into stages.
- **Stages**: a job is split into stages at points where data has to be shuffled between machines (more on this in Part 7). Each stage's tasks, duration, and data read/written are shown here.
- **Storage**: shows any RDDs or DataFrames you've explicitly cached (Part 8 covers this).
- **Executors**: memory and CPU usage per executor — crucial for diagnosing performance problems.
- **SQL / DataFrame**: for DataFrame and SQL workloads (starting Part 4), shows the physical execution plan Catalyst generated.

Get comfortable clicking around this UI now. Every later post in this series will refer back to it.

## What's next

We used RDDs today without really explaining them. In Part 3, we'll slow down and go deep: what transformations and actions actually are, the difference between narrow and wide transformations, and why lazy evaluation exists in the first place.

---

*Next in the series: [RDDs: The Original Building Block]({{ '/posts/2026/07/16/spark-rdds/' | relative_url }}).*
