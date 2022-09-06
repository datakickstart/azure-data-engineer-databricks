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

create_database("refined", refined_base_path)

# COMMAND ----------


#Area table
area_raw_df = spark.read.format(raw_format).load(raw_base_path + "/area")
area_refined = area_raw_df.select("area_code", "area_name", "display_level")
area_refined.write.format(refined_format).save(refined_base_path + "area")

# COMMAND ----------

refined_df =spark.read.format(refined_format).load(refined_base_path + "area")
display(refined_df.orderBy("area_code"))


# COMMAND ----------

# Series table
series_raw_df = spark.read.format(raw_format).load(raw_base_path + "/series")
series_refined = series_raw_df.drop("footnote_codes").withColumn("lastRefreshed", lit(load_time)
series_refined.write.mode("overwrite").format(refined_format).save(refined_base_path + "series")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE refined_cu;

# COMMAND ----------

# Current table
current_raw_df = spark.read.format(raw_format).load(raw_base_path + "/current")
current_refined = current_raw_df.select("*").drop("footnote_codes")
current_refined.write.mode("overwrite").option("delta.enableChangeDataFeed","true").format(refined_format).saveAsTable("refined_cu.current")

# COMMAND ----------


