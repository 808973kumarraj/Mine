# Brief: How Apache Spark works

**Angle:** Most Spark explainers list features ("fast! in-memory!"). This one follows ONE job, a word count,
from your code to the cluster and back, so you see *why* it's fast: lazy plans, stages split at shuffles, and data kept in memory.

| # | Claim | Source |
|---|-------|--------|
| 1 | Spark started at UC Berkeley's AMPLab in 2009 and became an Apache top-level project in 2014. | https://spark.apache.org/history.html |
| 2 | A Spark app has a driver program that runs your main function and coordinates executors on a cluster. | https://spark.apache.org/docs/latest/cluster-overview.html |
| 3 | Executors are processes on worker nodes that run tasks and keep data in memory or on disk. | https://spark.apache.org/docs/latest/cluster-overview.html |
| 4 | A cluster manager (Standalone, YARN, Kubernetes) hands out resources to the app. | https://spark.apache.org/docs/latest/cluster-overview.html |
| 5 | Data is split into partitions; each task works on one partition. | https://spark.apache.org/docs/latest/rdd-programming-guide.html |
| 6 | Transformations (map, filter) are lazy: they only record what to do. | https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations |
| 7 | Actions (count, collect, save) trigger the actual computation. | https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations |
| 8 | Spark builds a DAG of stages; the DAG scheduler splits stages at shuffle boundaries. | https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations ; Zaharia et al. 2012 (NSDI) https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf |
| 9 | A shuffle redistributes data across partitions (e.g. reduceByKey) and is expensive: disk I/O, serialization, network. | https://spark.apache.org/docs/latest/rdd-programming-guide.html#performance-impact |
| 10 | Datasets can be cached in memory so later operations reuse them. | https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence |
| 11 | Lost partitions are rebuilt from lineage (the recorded transformations), not from replicas. | Zaharia et al. 2012 (NSDI), link above |
| 12 | The RDD paper reported up to 20x faster than Hadoop for iterative apps. | Zaharia et al. 2012 (NSDI), abstract |
| 13 | DataFrame/SQL queries go through the Catalyst optimizer before running. | https://spark.apache.org/docs/latest/sql-programming-guide.html ; https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf |

## Illustrative (made up for the visuals, not claimed as fact)
- A 3-line text file "the cat sat / the dog sat / the cat ran" split into 3 partitions.
- A cluster drawn with 1 driver and 3 executors.
