---
layout: post
title: "Deploying Spark to Production"
description: "The finale of the series — cluster managers compared (Standalone, YARN, Kubernetes), client vs. cluster deploy modes, key spark-submit flags, and a checklist for running Spark for real."
date: 2026-07-23
tags: [spark, big-data, tutorial, pyspark, kubernetes, devops]
---

{% include spark-series-nav.html part=10 %}

We've covered the whole engine across this series: RDDs, DataFrames, SQL, performance tuning, and streaming, all running in `local[*]` mode. This final post covers what changes when you point that same code at a real cluster.

The good news, and the point worth remembering from [Part 1]({{ '/posts/2026/07/14/spark-101-introduction/' | relative_url }}): the driver/executor architecture is identical in local mode and on a 1,000-node cluster. Nothing about the code you've written this series needs to change — only how you launch it.

## Cluster managers

Spark needs something to hand out resources (CPU, memory) across machines and launch executor processes. Three real options:

**Standalone** — Spark's own built-in cluster manager. Simplest to set up (a master process plus worker processes, nothing else required), good for a dedicated Spark cluster with no other workloads competing for the same hardware. Less commonly used in larger organizations that already run other systems needing resource management.

**YARN** — Hadoop's resource manager. The traditional choice in organizations already running a Hadoop ecosystem (HDFS, Hive), since Spark integrates directly with existing HDFS storage and shares cluster resources with other YARN applications.

**Kubernetes** — the increasingly common default for new deployments. Spark runs as pods, which means it shares infrastructure, tooling, and operational practices with everything else already running on Kubernetes, rather than requiring a separate cluster just for Spark.

```
                      Your spark-submit command
                                │
                    ┌───────────┼────────────┐
                    ▼            ▼            ▼
              Standalone       YARN      Kubernetes
              ┌─────────┐  ┌─────────┐  ┌─────────┐
              │ Spark   │  │Resource │  │  K8s    │
              │ Master  │  │ Manager │  │  API    │
              └────┬────┘  └────┬────┘  └────┬────┘
                   │            │            │
              ┌────▼────┐  ┌────▼────┐  ┌────▼────┐
              │ Worker  │  │NodeMgr  │  │  Pod    │
              │processes│  │+executor│  │executors│
              └─────────┘  └─────────┘  └─────────┘
```

## Client vs. cluster deploy mode

Independent of which cluster manager you use, you choose where the **driver** process runs:

- **Client mode**: the driver runs on the machine you launched `spark-submit` from — your laptop, a jump box, wherever. Good for interactive work and debugging, since driver logs print directly to your terminal. Risky for production: if your machine loses network connectivity or you close the terminal, the job dies with it.
- **Cluster mode**: the driver itself runs as a process inside the cluster, submitted alongside the executors. The client machine can disconnect entirely once the job is submitted — the job keeps running independently. This is the standard choice for scheduled production jobs.

```bash
# Client mode: driver runs locally, useful while developing
spark-submit --deploy-mode client --master yarn my_job.py

# Cluster mode: driver runs on the cluster, standard for production
spark-submit --deploy-mode cluster --master yarn my_job.py
```

## Key `spark-submit` flags

```bash
spark-submit \
  --master k8s://https://my-cluster-api:6443 \
  --deploy-mode cluster \
  --name my-etl-job \
  --executor-memory 4g \
  --executor-cores 4 \
  --num-executors 20 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.adaptive.enabled=true \
  --py-files dependencies.zip \
  my_job.py --input s3://bucket/input --output s3://bucket/output
```

- `--executor-memory` / `--executor-cores`: resources per executor process. From Part 8, this directly affects how much spill you'll see and how much parallelism each executor offers.
- `--num-executors`: total executor count (static — see below for dynamic allocation).
- `--conf`: any Spark configuration property, including the tuning knobs from Part 8.
- `--py-files`: ship a zip of your own Python modules/dependencies to executors, since they don't share a filesystem with the driver by default.
- Anything after the script name is passed through as regular command-line arguments to your script.

## Dynamic allocation

Rather than a fixed `--num-executors`, let Spark scale executor count up and down based on actual workload:

```bash
spark-submit \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=2 \
  --conf spark.dynamicAllocation.maxExecutors=50 \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  my_job.py
```

This matters most for shared clusters and variable workloads — a job that's mostly light with occasional heavy stages doesn't need to hold 50 executors idle the whole time it runs.

## Monitoring beyond the local UI

The `localhost:4040` UI from Part 2 only exists while a job is actively running, on the driver's machine — not practical for production jobs running unattended on a cluster. Two things fix that:

- **Spark History Server**: a separate process that reads event logs written by completed applications and serves the same UI after the fact, so you can debug a job that finished (or failed) hours ago. Enable event logging with `spark.eventLog.enabled=true` and a shared log directory.
- **Metrics integration**: Spark can emit metrics to Graphite, Prometheus, and similar via the metrics system (`spark.metrics.conf`), so executor memory, GC time, and shuffle stats show up in whatever monitoring stack your team already uses, alongside alerting.

## A pre-production checklist

Before scheduling any Spark job to run unattended and regularly:

1. **Deploy mode**: `cluster`, not `client`, so the job survives the submitting machine disconnecting.
2. **Resource sizing**: based on actual data volume, not guesswork — check the UI/History Server after a first real run and adjust.
3. **Idempotency**: if the job fails partway and reruns, does it double-write data? Prefer `overwrite` or partition-level overwrite over blind `append` for anything that might rerun.
4. **Logging and alerting**: event logging enabled, and a monitoring hook so failures actually page someone rather than failing silently.
5. **Retry and timeout policy**: set at the orchestration layer (Airflow, Dagster, or whatever schedules the job), not left to Spark's defaults.
6. **Data validation**: a lightweight row-count or schema check after the job, so a silently-wrong output (as opposed to a crash) gets caught.

## Series wrap-up

Across ten parts, we went from "what problem does Spark solve" to a job running unattended on a real cluster: the RDD foundation, DataFrames and SQL on top of it, reading and writing real data formats, joins and window functions, the execution model that ties it all together, tuning based on that model, streaming with the same API, and finally, deploying it properly.

The throughline worth keeping in your head: everything in Spark — batch, SQL, streaming, MLlib included — runs on the same driver/executor engine, the same lazy DAG-based scheduling, and the same shuffle-is-expensive cost model. Once that clicks, new Spark features are mostly new APIs on a system you already understand.

---

*This is Part 10 of the Learning Apache Spark series. Start from the beginning with [What Spark Is and Why It Exists]({{ '/posts/2026/07/14/spark-101-introduction/' | relative_url }}).*
