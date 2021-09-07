# Databricks notebook source
# MAGIC %sql 
# MAGIC -- Review the cleaned up clickstream data
# MAGIC SELECT * FROM My_Triggered_Wikipedia_Table.clickstream_clean

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Review the top referrers to Wikipedia's Apache Spark articles
# MAGIC SELECT * FROM My_Triggered_Wikipedia_Table.top_spark_referers
