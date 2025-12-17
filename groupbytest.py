#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession, functions as F

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=5_000_000, help="How many rows to generate")
    p.add_argument("--keys", type=int, default=100, help="Number of groups (key cardinality)")
    p.add_argument("--partitions", type=int, default=200, help="Shuffle/repartition partitions")
    args = p.parse_args()

    spark = SparkSession.builder.appName("groupbytest").getOrCreate()
    sc = spark.sparkContext

    print("Master:", sc.master)
    print("AppId:", sc.applicationId)

    # Generate synthetic data (no external storage needed)
    df = (
        spark.range(0, args.rows)
        .select(
            (F.col("id") % F.lit(args.keys)).alias("k"),
            F.col("id").alias("v")
        )
        # Force distribution + shuffle
        .repartition(args.partitions, "k")
    )

    # GroupBy aggregation
    out = (
        df.groupBy("k")
          .agg(
              F.count("*").alias("cnt"),
              F.sum("v").alias("sum_v"),
              F.avg("v").alias("avg_v"),
          )
          .orderBy("k")
    )

    out.show(50, truncate=False)

    # Useful to confirm executors (worker) are actually being used
    print("Executors (block managers) memory status:")
    jmap = sc._jsc.sc().getExecutorMemoryStatus()   # scala.collection.Map[String, (Long, Long)]

    it = jmap.iterator()  # iterator over (String, (Long, Long))
    while it.hasNext():
        kv = it.next()          # scala.Tuple2(key, value)
        hostport = kv._1()
        mem = kv._2()           # scala.Tuple2(maxMem, remainingMem)
        max_mem = mem._1()
        remaining = mem._2()
        print(f" - {hostport} max={max_mem} remaining={remaining}")

    spark.stop()

if __name__ == "__main__":
    main()
