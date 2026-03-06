#`SparkSession` is the entry point to programming with Spark. It provides a single point of entry to interact with Spark functionality
# and to create DataFrame and DataSet.
# Use `SparkSession` when working with high-level APIs such as DataFrame and SQL. Ideal for most data processing tasks because of its simplicity and unified interface.
# Use `SparkContext` for low-level RDD operations. Necessary when you need more control over the underlying Spark execution or need to manipulate RDDs directly.
from pyspark.sql import SparkSession


## Kryo serialization
# Kryo is a more efficient (faster and more compact) serialization format than the default Java serializer, which can improve performance when shuffling data or caching RDDs/DataFrames.

## Hive dynamic partitioning
# Dynamic partitioning allows you to insert data into a partitioned table without specifying the partition values
# AWS Glue Data Catalog: persistent, external metastore for Hive tables.
# Nonstrict mode: allows dynamic partitioning without requiring at least one static partition column.
# This means you can insert data into a partitioned table without specifying any partition values,
# and Spark will automatically create the necessary partitions based on the data being inserted.

## Dynamic partitioning
# Partitioning means dividing data into chunks based on specific column values.
# This division makes it easier to handle large datasets by speeding up queries and reducing the amount of data processed at one time.
# When INSERT OVERWRITE a partitioned data source table:
# static mode: Spark deletes all the partitions that match the partition specification in the INSERT statement, before overwriting
# dynamic mode: Spark doesn't delete partitions ahead, and overwrite only the partitions that are being written to at runtime

## Adaptive query enabled
# When true, enable adaptive query execution, which re-optimizes the query plan in the middle of query execution, based on accurate runtime statistics.

## Coalesce partitions
# Spark will coalesce contiguous shuffle partitions according to the target size.

## Optimize Skews In Rebalance Partitions
# Spark will optimize the skewed shuffle partitions in RebalancePartitions and split them to smaller ones according to the target size.

spark = SparkSession.builder \
    .appName("Configs") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()
