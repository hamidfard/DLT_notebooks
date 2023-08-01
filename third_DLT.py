# Databricks notebook source
# MAGIC %md
# MAGIC Mixing streaming tables and materialized views into a single pipeline

# COMMAND ----------

import dlt

@dlt.table
def streaming_bronze():
  return (
    # Since this is a streaming source, this table is incremental.
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load("/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json")
  )

@dlt.table
def streaming_silver():
  # Since we read the bronze table as a stream, this silver table is also updated incrementally.
  return dlt.read_stream("streaming_bronze").where("prev_id IS NOT NULL")

@dlt.table
def live_gold():
  # This table will be recomputed completely by reading the whole silver table when it is updated.
  return dlt.read("streaming_silver").groupBy("type").count()

# COMMAND ----------


