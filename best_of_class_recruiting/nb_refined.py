# Databricks notebook source
# MAGIC %run ../utils/mount_storage

# COMMAND ----------

from pyspark.sql.functions import lit, col
from datetime import datetime

load_time = datetime.now()
raw_base_path = dbutils.secrets.get("demo", "raw-datalake-path") + "cu"
refined_base_path = dbutils.secrets.get("demo", "refined-datalake-path") + "cu"
raw_format = "parquet"
refined_format = "delta"

adls_authenticate()

# COMMAND ----------

def create_database(db_name, path, drop=False):
    if drop:
        spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE;")    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{path}'")

# create_database("refined", refined_base_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prep source and target tables (for demo only)

# COMMAND ----------


## Area table
# area_raw_df = spark.read.format(raw_format).load(raw_base_path + "/area_original")
# area_refined = area_raw_df.select("area_code", "area_name", "display_level").withColumn("is_deleted", lit(False))
# area_refined.write.format(refined_format).mode("overwrite").option("enableChangeDataFeed","true").save(refined_base_path + "/area")

# COMMAND ----------

## Series table
# series_raw_df = spark.read.format(raw_format).load(raw_base_path + "/series")
# series_refined = series_raw_df.drop("footnote_codes").withColumn("lastRefreshed", lit(load_time)
# series_refined.write.mode("overwrite").format(refined_format).save(refined_base_path + "series")

# COMMAND ----------

## Current table
# current_raw_df = spark.read.format(raw_format).load(raw_base_path + "/current")
# current_refined = current_raw_df.select("*").drop("footnote_codes")
# current_refined.write.mode("overwrite").option("delta.enableChangeDataFeed","true").format(refined_format).saveAsTable("refined_cu.current")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Delete records (for demo purposes)

# COMMAND ----------

# # DELETE records for testing and demo purposes
# raw_df2 = df_source.filter((col('area_code') != 'S35A') & (col('area_code') != 'S300'))
# raw_df2.write.format(raw_format).mode("overwrite").save(raw_base_path + "/area")

# spark.sql(f'REFRESH TABLE "{raw_base_path}/area"')
# df_source = spark.read.format(raw_format).load(raw_base_path + "/area").select("area_code", "area_name", "display_level").withColumn("is_deleted", lit(False))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load refined tables concurrently

# COMMAND ----------

status = dbutils.notebook.run("nb_refined_table_load", 60, arguments={"table": "area", "id_column": "area_code", "columns": "area_code,area_name,display_level" })
print(status)
if status != 'success':
    raise Exception(f"Failed to lo")

# COMMAND ----------

from threading import Thread
from queue import Queue

q = Queue()

worker_count = 2

def run_tasks(function, q):
    while not q.empty():
        value = q.get()
        function(value)
        q.task_done()


print(table_list)

for table in table_list:
    q.put(table)

for i in range(worker_count):
    t=Thread(target=run_tasks, args=(load_table, q))
    t.daemon = True
    t.start()

q.join()

# COMMAND ----------

# display(delta_target.history())
refined_df =spark.read.format("delta").option("readChangeData", True).option("startingVersion",1).load(f"{refined_base_path}/{table}")
display(refined_df.orderBy("area_code"))


# COMMAND ----------


